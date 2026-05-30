import { Component, inject, signal, computed, OnInit, OnDestroy, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { forkJoin, interval, timer, Subscription, of } from 'rxjs';
import { switchMap, catchError } from 'rxjs/operators';
import { WatchlistService } from '../../../core/services/watchlist.service';
import { StockService } from '../../../core/services/stock.service';
import { CompanyService } from '../../../core/services/company.service';
import { WatchlistResponse } from '../../../core/models/watchlist.model';
import { StockResponse, StockTickResponse } from '../../../core/models/stock.model';
import { CompanyResponse } from '../../../core/models/company.model';

interface WatchRow extends StockResponse {
    currentPrice: number;
    liveTime:     string;
    isLive:       boolean;
}

type SortField = keyof WatchRow;
type SortDir   = 'asc' | 'desc';

@Component({
    selector: 'app-watchlist',
    standalone: true,
    imports: [CommonModule, FormsModule, RouterLink],
    templateUrl: './watchlist.component.html',
    styleUrl: './watchlist.component.css'
})
export class WatchlistComponent implements OnInit, OnDestroy {
    private wlSvc      = inject(WatchlistService);
    private static lastFetchTime = 0;
    private stockSvc   = inject(StockService);
    private companySvc = inject(CompanyService);

    // ── Watchlist state ──
    watchedSymbols = signal<string[]>([]);
    rows           = signal<WatchRow[]>([]);
    allStocks      = signal<StockResponse[]>([]);   // full EOD list
    loading        = signal(true);
    error          = signal('');
    deletingSet    = signal<Set<string>>(new Set());

    // ── Sorting ──
    sortField = signal<SortField>('symbol');
    sortDir   = signal<SortDir>('asc');

    sorted = computed(() => {
        const sf = this.sortField();
        const sd = this.sortDir();
        return [...this.rows()].sort((a, b) => {
            const av: any = (a as any)[sf] ?? '';
            const bv: any = (b as any)[sf] ?? '';
            const cmp = av < bv ? -1 : av > bv ? 1 : 0;
            return sd === 'asc' ? cmp : -cmp;
        });
    });

    // ── Polling ──
    private pollSub?:      Subscription;
    private countdownSub?: Subscription;
    readonly POLL_MS = 60_000;
    nextRefresh  = signal(60);
    isRefreshing = signal(false);

    // ── Search / autocomplete ──
    private _searchQuery = signal('');
    get searchQuery()    { return this._searchQuery(); }
    set searchQuery(v)   { this._searchQuery.set(v);  }

    allCompanies = signal<CompanyResponse[]>([]);
    showDropdown = signal(false);
    addLoading   = signal(false);
    addError     = signal('');
    activeIndex  = signal(-1);

    suggestions = computed(() => {
        const q = this._searchQuery().trim().toLowerCase();
        if (!q) return [];
        const watched = new Set(this.watchedSymbols());
        return this.allCompanies()
            .filter(c =>
                !watched.has(c.symbol) &&
                (c.symbol.toLowerCase().includes(q) || c.companyName.toLowerCase().includes(q))
            )
            .slice(0, 8);
    });

    ngOnInit() {
        // Load watchlist + all stocks in parallel
        forkJoin({
            watchlist: this.wlSvc.getWatchlist().pipe(catchError(() => of([] as WatchlistResponse[]))),
            stocks:    this.stockSvc.findStocks().pipe(catchError(() => of([] as StockResponse[])))
        }).subscribe({
            next: ({ watchlist, stocks }) => {
                this.allStocks.set(stocks);
                const symbols = watchlist.map(w => w.symbol);
                this.watchedSymbols.set(symbols);
                this.rows.set(this.buildRows(symbols, stocks));
                this.loading.set(false);
                if (symbols.length) this.startPricePolling(symbols);
            },
            error: () => { this.error.set('Không thể tải watchlist.'); this.loading.set(false); }
        });

        // Load companies for autocomplete
        this.companySvc.findCompanies().subscribe({
            next: data => this.allCompanies.set(data),
            error: ()  => {}
        });
    }

    // ── Build WatchRow from symbol list + EOD stocks ──
    private buildRows(symbols: string[], stocks: StockResponse[]): WatchRow[] {
        const stockMap = new Map(stocks.map(s => [s.symbol, s]));
        return symbols
            .map(sym => stockMap.get(sym))
            .filter((s): s is StockResponse => !!s)
            .map(s => ({ ...s, currentPrice: s.close, liveTime: '', isLive: false }));
    }

    // ── Price polling ──
    // force=true bypasses the cooldown (used when the symbol set changes).
    private startPricePolling(symbols: string[], force = false) {
        this.pollSub?.unsubscribe();
        this.countdownSub?.unsubscribe();

        const elapsed = Date.now() - WatchlistComponent.lastFetchTime;
        const delay   = (force || elapsed >= this.POLL_MS) ? 0 : this.POLL_MS - elapsed;

        this.nextRefresh.set(delay === 0 ? 60 : Math.ceil(delay / 1000));

        this.pollSub = timer(delay, this.POLL_MS).pipe(
            switchMap(() => {
                this.isRefreshing.set(true);
                return this.stockSvc.findLatestBatch(symbols).pipe(catchError(() => of([])));
            })
        ).subscribe({
            next: ticks => {
                WatchlistComponent.lastFetchTime = Date.now();
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
        if (!ticks.length) return;
        const tickMap = new Map(ticks.map(t => [t.symbol, t]));
        this.rows.update(rows => rows.map(r => {
            const tick = tickMap.get(r.symbol);
            return tick ? { ...r, currentPrice: tick.price, liveTime: tick.eventTime, isLive: true } : r;
        }));
    }

    // ── Autocomplete ──
    onSearchInput() {
        this.addError.set('');
        this.activeIndex.set(-1);
        this.showDropdown.set(this.suggestions().length > 0);
    }

    selectSuggestion(company: CompanyResponse) {
        this._searchQuery.set('');
        this.showDropdown.set(false);
        this.activeIndex.set(-1);
        this.doAdd(company.symbol);
    }

    onKeydown(e: KeyboardEvent) {
        const list = this.suggestions();
        if (!this.showDropdown() || !list.length) return;
        if      (e.key === 'ArrowDown') { e.preventDefault(); this.activeIndex.update(i => Math.min(i + 1, list.length - 1)); }
        else if (e.key === 'ArrowUp')   { e.preventDefault(); this.activeIndex.update(i => Math.max(i - 1, -1)); }
        else if (e.key === 'Enter')     { e.preventDefault(); const idx = this.activeIndex(); if (idx >= 0) this.selectSuggestion(list[idx]); }
        else if (e.key === 'Escape')    { this.showDropdown.set(false); }
    }

    @HostListener('document:click', ['$event'])
    onDocumentClick(e: MouseEvent) {
        if (!(e.target as HTMLElement).closest('.search-container')) this.showDropdown.set(false);
    }

    // ── Add / Remove ──
    private doAdd(symbol: string) {
        this.addLoading.set(true);
        this.addError.set('');
        this.wlSvc.addSymbol(symbol).subscribe({
            next: watchlist => {
                const symbols = watchlist.map(w => w.symbol);
                this.watchedSymbols.set(symbols);
                this.rows.set(this.buildRows(symbols, this.allStocks()));
                this.addLoading.set(false);
                this.startPricePolling(symbols, true);
            },
            error: err => { this.addError.set(err?.error?.message || `Không thể thêm ${symbol}`); this.addLoading.set(false); }
        });
    }

    removeSymbol(symbol: string) {
        this.deletingSet.update(s => { const n = new Set(s); n.add(symbol); return n; });
        this.wlSvc.removeSymbol(symbol).subscribe({
            next: watchlist => {
                const symbols = watchlist.map(w => w.symbol);
                this.watchedSymbols.set(symbols);
                this.rows.set(this.buildRows(symbols, this.allStocks()));
                this.deletingSet.update(s => { const n = new Set(s); n.delete(symbol); return n; });
                if (symbols.length) this.startPricePolling(symbols);
                else { this.pollSub?.unsubscribe(); this.countdownSub?.unsubscribe(); }
            },
            error: err => {
                this.error.set(err?.error?.message || `Không thể xóa ${symbol}`);
                this.deletingSet.update(s => { const n = new Set(s); n.delete(symbol); return n; });
            }
        });
    }

    // ── Sort ──
    sort(field: SortField) {
        if (this.sortField() === field) this.sortDir.set(this.sortDir() === 'asc' ? 'desc' : 'asc');
        else { this.sortField.set(field); this.sortDir.set('asc'); }
    }

    icon(field: SortField): string {
        if (this.sortField() !== field) return '↕';
        return this.sortDir() === 'asc' ? '↑' : '↓';
    }

    // ── Formatters ──
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

    fmtTime(s: string | null | undefined): string {
        if (!s) return '—';
        return new Date(s).toLocaleString('vi-VN');
    }

    ngOnDestroy() {
        this.pollSub?.unsubscribe();
        this.countdownSub?.unsubscribe();
    }
}
