import { Component, inject, signal, computed, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { interval, timer, Subscription, of } from 'rxjs';
import { switchMap, catchError } from 'rxjs/operators';
import { StockService } from '../../../core/services/stock.service';
import { StockResponse, StockTickResponse } from '../../../core/models/stock.model';

type SortField = 'symbol' | 'companyName' | 'exchange' | 'sector' | 'industry'
               | 'open' | 'high' | 'low' | 'currentPrice' | 'volume'
               | 'dailyReturn' | 'volatility';
type SortDir = 'asc' | 'desc';

export interface StockRow extends StockResponse {
    currentPrice: number;   // livePrice ?? close
    liveTime:     string;   // eventTime from tick ('' if no live data)
    isLive:       boolean;
}

@Component({
    selector: 'app-stock-list',
    standalone: true,
    imports: [CommonModule, FormsModule, RouterLink],
    templateUrl: './stock-list.component.html',
    styleUrl: './stock-list.component.css'
})
export class StockListComponent implements OnInit, OnDestroy {
    private stockSvc = inject(StockService);
    private static lastFetchTime = 0;

    rows    = signal<StockRow[]>([]);
    loading = signal(true);
    error   = signal('');

    // polling
    private pollSub?:      Subscription;
    private countdownSub?: Subscription;
    readonly POLL_MS = 60_000;
    nextRefresh  = signal(60);
    isRefreshing = signal(false);

    // filters
    private _search   = signal('');
    private _sector   = signal('');
    private _exchange = signal('');
    get searchQuery()       { return this._search();   }
    set searchQuery(v)      { this._search.set(v);     }
    get selectedSector()    { return this._sector();   }
    set selectedSector(v)   { this._sector.set(v);     }
    get selectedExchange()  { return this._exchange();  }
    set selectedExchange(v) { this._exchange.set(v);   }

    sortField = signal<SortField>('symbol');
    sortDir   = signal<SortDir>('asc');

    sectors   = computed(() => [...new Set(this.rows().map(r => r.sector).filter(Boolean))].sort());
    exchanges = computed(() => [...new Set(this.rows().map(r => r.exchange).filter(Boolean))].sort());

    filtered = computed(() => {
        const q  = this._search().toLowerCase();
        const sec = this._sector();
        const ex  = this._exchange();
        const sf  = this.sortField();
        const sd  = this.sortDir();

        let list = this.rows().filter(r =>
            (!q   || r.symbol.toLowerCase().includes(q) || r.companyName.toLowerCase().includes(q)) &&
            (!sec || r.sector   === sec) &&
            (!ex  || r.exchange === ex)
        );

        return [...list].sort((a, b) => {
            const av: any = (a as any)[sf] ?? '';
            const bv: any = (b as any)[sf] ?? '';
            const cmp = av < bv ? -1 : av > bv ? 1 : 0;
            return sd === 'asc' ? cmp : -cmp;
        });
    });

    ngOnInit() {
        // Load company + EOD data once
        this.stockSvc.findStocks().subscribe({
            next: (stocks: StockResponse[]) => {
                this.rows.set(stocks.map(s => this.toRow(s, null)));
                this.loading.set(false);
                this.startPricePolling(stocks.map(s => s.symbol));
            },
            error: () => { this.error.set('Không thể tải danh sách cổ phiếu.'); this.loading.set(false); }
        });
    }

    private startPricePolling(symbols: string[]) {
        const elapsed  = Date.now() - StockListComponent.lastFetchTime;
        const delay    = elapsed >= this.POLL_MS ? 0 : this.POLL_MS - elapsed;

        this.nextRefresh.set(delay === 0 ? 60 : Math.ceil(delay / 1000));

        // timer(delay, period): fires after `delay` ms then every POLL_MS.
        // delay=0 → fetch immediately (data stale); delay>0 → wait remainder.
        this.pollSub = timer(delay, this.POLL_MS).pipe(
            switchMap(() => {
                this.isRefreshing.set(true);
                return this.stockSvc.findLatestBatch(symbols).pipe(catchError(() => of([])));
            })
        ).subscribe({
            next: (ticks) => {
                StockListComponent.lastFetchTime = Date.now();
                this.applyTicks(ticks as StockTickResponse[]);
                this.isRefreshing.set(false);
                this.nextRefresh.set(60);
            }
        });

        this.countdownSub = interval(1_000).subscribe(() =>
            this.nextRefresh.update(n => n > 1 ? n - 1 : 60)
        );
    }

    private applyTicks(ticks: StockTickResponse[]) {
        const tickMap = new Map(ticks.map(t => [t.symbol, t]));
        this.rows.update(rows => rows.map(r => {
            const tick = tickMap.get(r.symbol);
            return tick
                ? { ...r, currentPrice: tick.price, liveTime: tick.eventTime, isLive: true }
                : r;
        }));
    }

    private toRow(s: StockResponse, tick: StockTickResponse | null): StockRow {
        return {
            ...s,
            currentPrice: tick ? tick.price : s.close,
            liveTime:     tick ? tick.eventTime : '',
            isLive:       !!tick
        };
    }

    sort(field: SortField) {
        if (this.sortField() === field) this.sortDir.set(this.sortDir() === 'asc' ? 'desc' : 'asc');
        else { this.sortField.set(field); this.sortDir.set('asc'); }
    }

    icon(field: SortField): string {
        if (this.sortField() !== field) return '↕';
        return this.sortDir() === 'asc' ? '↑' : '↓';
    }

    fmt(n: number | null | undefined, dec = 2): string {
        if (n == null) return '—';
        return n.toLocaleString('vi-VN', { minimumFractionDigits: dec, maximumFractionDigits: dec });
    }

    fmtVol(n: number | null | undefined): string {
        if (n == null) return '—';
        if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
        if (n >= 1_000)     return (n / 1_000).toFixed(0) + 'K';
        return n.toString();
    }

    ngOnDestroy() {
        this.pollSub?.unsubscribe();
        this.countdownSub?.unsubscribe();
    }
}
