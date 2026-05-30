import {
    Component, inject, signal, computed, OnInit, OnDestroy,
    ViewChild, ElementRef
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { forkJoin, interval, timer, Subscription, of } from 'rxjs';
import { switchMap, catchError, tap } from 'rxjs/operators';
import {
    createChart, IChartApi, ISeriesApi, SeriesType,
    LineSeries, CandlestickSeries, HistogramSeries,
    ColorType, CrosshairMode, LineStyle
} from 'lightweight-charts';
import { StockService } from '../../../../core/services/stock.service';
import { StockResponse, StockTickResponse, StockOhlcResponse } from '../../../../core/models/stock.model';

type Tab = 'overview' | 'daily' | 'ohlc';

@Component({
    selector: 'app-stock-detail',
    standalone: true,
    imports: [CommonModule, RouterLink],
    templateUrl: './stock-detail.component.html',
    styleUrl: './stock-detail.component.css'
})
export class StockDetailComponent implements OnInit, OnDestroy {
    private route    = inject(ActivatedRoute);
    private stockSvc = inject(StockService);

    private static symbolCache = new Map<string, {
        stock:     StockResponse;
        daily:     StockResponse[];
        ohlc:      StockOhlcResponse[];
        latest:    StockTickResponse | null;
        fetchedAt: number;
    }>();

    symbol = signal('');
    tab    = signal<Tab>('overview');

    stock   = signal<StockResponse | null>(null);
    daily   = signal<StockResponse[]>([]);
    ohlc    = signal<StockOhlcResponse[]>([]);
    latest  = signal<StockTickResponse | null>(null);

    loading = signal(true);
    error   = signal('');

    @ViewChild('lineChartRef')   lineChartRef!:   ElementRef<HTMLDivElement>;
    @ViewChild('volChartRef')    volChartRef!:    ElementRef<HTMLDivElement>;
    @ViewChild('candleChartRef') candleChartRef!: ElementRef<HTMLDivElement>;

    private lineChart?:    IChartApi;
    private volChart?:     IChartApi;
    private candleChart?:  IChartApi;
    private lineSeries?:   ISeriesApi<SeriesType>;
    private volSeries?:    ISeriesApi<SeriesType>;
    private candleSeries?: ISeriesApi<SeriesType>;

    private pollSub?:      Subscription;
    private ohlcPollSub?: Subscription;
    readonly POLL_MS = 60_000;
    nextRefresh = signal(60);
    private countdownSub?: Subscription;

    ohlcInterval = signal<'1m' | '5m' | '15m'>('1m');

    filteredOhlc = computed(() => {
        const raw = this.ohlc();
        const iv  = this.ohlcInterval();
        if (iv === '1m') return raw;
        const mins = iv === '5m' ? 5 : 15;
        const groups = new Map<string, StockOhlcResponse[]>();
        raw.forEach(r => {
            const d = new Date(r.windowStart);
            d.setMinutes(Math.floor(d.getMinutes() / mins) * mins, 0, 0);
            const key = d.toISOString();
            if (!groups.has(key)) groups.set(key, []);
            groups.get(key)!.push(r);
        });
        return [...groups.entries()].map(([, arr]) => ({
            ...arr[0],
            open:   arr[0].open,
            high:   Math.max(...arr.map(a => a.high)),
            low:    Math.min(...arr.map(a => a.low)),
            close:  arr[arr.length - 1].close,
            volume: arr.reduce((s, a) => s + a.volume, 0),
            value:  arr.reduce((s, a) => s + a.value, 0)
        }));
    });

    priceChange = computed(() => {
        const d = this.daily();
        if (d.length < 2) return null;
        const first = d[0].close, last = d[d.length - 1].close;
        return { abs: last - first, pct: ((last - first) / first) * 100 };
    });

    ngOnInit() {
        this.route.paramMap.subscribe(params => {
            const sym = params.get('symbol') ?? '';
            this.symbol.set(sym);
            this.loadData(sym);
        });
    }

    private loadData(symbol: string) {
        this.loading.set(true);
        this.error.set('');

        const cached  = StockDetailComponent.symbolCache.get(symbol);
        const elapsed = cached ? Date.now() - cached.fetchedAt : Infinity;

        // Re-use cached data if still within the poll window
        if (cached && elapsed < this.POLL_MS) {
            this.stock.set(cached.stock);
            this.daily.set(cached.daily);
            this.ohlc.set(cached.ohlc);
            this.latest.set(cached.latest);
            this.loading.set(false);
            this.startPolling(symbol);
            return;
        }

        forkJoin({
            stock:  this.stockSvc.findStock(symbol).pipe(catchError(() => of(null))),
            daily:  this.stockSvc.findDaily(symbol).pipe(catchError(() => of([]))),
            ohlc:   this.stockSvc.findOhlc(symbol).pipe(catchError(() => of([]))),
            latest: this.stockSvc.findLatest(symbol).pipe(catchError(() => of(null)))
        }).subscribe({
            next: ({ stock, daily, ohlc, latest }) => {
                if (!stock) { this.error.set(`Không tìm thấy mã ${symbol}.`); this.loading.set(false); return; }
                const sortedDaily = (daily as StockResponse[]).sort((a, b) => a.tradingDate.localeCompare(b.tradingDate));
                const sortedOhlc  = (ohlc  as StockOhlcResponse[]).sort((a, b) => a.windowStart.localeCompare(b.windowStart));
                this.stock.set(stock);
                this.daily.set(sortedDaily);
                this.ohlc.set(sortedOhlc);
                this.latest.set(latest as StockTickResponse | null);
                StockDetailComponent.symbolCache.set(symbol, {
                    stock, daily: sortedDaily, ohlc: sortedOhlc,
                    latest: latest as StockTickResponse | null,
                    fetchedAt: Date.now()
                });
                this.loading.set(false);
                this.startPolling(symbol);
            },
            error: () => { this.error.set('Lỗi kết nối đến server.'); this.loading.set(false); }
        });
    }

    private startPolling(symbol: string) {
        this.pollSub?.unsubscribe();
        this.countdownSub?.unsubscribe();
        this.ohlcPollSub?.unsubscribe();

        const cached  = StockDetailComponent.symbolCache.get(symbol);
        const elapsed = cached ? Date.now() - cached.fetchedAt : Infinity;
        const delay   = elapsed >= this.POLL_MS ? 0 : this.POLL_MS - elapsed;

        this.nextRefresh.set(delay === 0 ? 60 : Math.ceil(delay / 1000));

        // ── Realtime Widget: poll /latest ──
        this.pollSub = timer(delay, this.POLL_MS).pipe(
            switchMap(() => this.stockSvc.findLatest(symbol).pipe(catchError(() => of(null))))
        ).subscribe({
            next: t => {
                if (t) {
                    const tick = t as StockTickResponse;
                    this.latest.set(tick);
                    const c = StockDetailComponent.symbolCache.get(symbol);
                    if (c) StockDetailComponent.symbolCache.set(symbol, { ...c, latest: tick, fetchedAt: Date.now() });
                }
                this.nextRefresh.set(60);
            }
        });

        // ── Countdown ──
        this.countdownSub = interval(1_000).subscribe(() =>
            this.nextRefresh.update(n => n > 1 ? n - 1 : 60)
        );

        // ── Candlestick Chart: poll /ohlc ──
        this.ohlcPollSub = timer(delay, this.POLL_MS).pipe(
            switchMap(() => this.stockSvc.findOhlc(symbol).pipe(catchError(() => of(null)))),
            tap(data => {
                if (!data) return;
                const sorted = (data as StockOhlcResponse[]).sort((a, b) => a.windowStart.localeCompare(b.windowStart));
                this.ohlc.set(sorted);
                const c = StockDetailComponent.symbolCache.get(symbol);
                if (c) StockDetailComponent.symbolCache.set(symbol, { ...c, ohlc: sorted });
            })
        ).subscribe({
            next: () => {
                if (this.tab() === 'ohlc') {
                    this.candleChart?.remove();
                    this.candleChart = undefined;
                    setTimeout(() => this.buildCandleChart(), 50);
                }
            }
        });
    }

    setTab(t: Tab) {
        this.tab.set(t);
        if (t === 'daily') setTimeout(() => this.buildDailyCharts(), 80);
        if (t === 'ohlc')  setTimeout(() => this.buildCandleChart(), 80);
    }

    setOhlcInterval(iv: '1m' | '5m' | '15m') {
        this.ohlcInterval.set(iv);
        this.candleChart?.remove();
        this.candleChart = undefined;
        setTimeout(() => this.buildCandleChart(), 50);
    }

    // ── Chart helpers ──
    private makeChart(el: HTMLElement, height = 280): IChartApi {
        return createChart(el, {
            width:  el.clientWidth || 800,
            height,
            layout: {
                background: { type: ColorType.Solid, color: '#0d0d0d' },
                textColor: 'rgba(255,255,255,0.5)'
            },
            grid: {
                vertLines: { color: 'rgba(255,255,255,0.04)' },
                horzLines: { color: 'rgba(255,255,255,0.04)' }
            },
            crosshair: { mode: CrosshairMode.Normal },
            rightPriceScale: { borderColor: 'rgba(255,255,255,0.1)' },
            timeScale: { borderColor: 'rgba(255,255,255,0.1)', timeVisible: true }
        });
    }

    private buildDailyCharts() {
        const daily = this.daily();
        if (!daily.length || !this.lineChartRef?.nativeElement || !this.volChartRef?.nativeElement) return;

        this.lineChart?.remove();
        this.volChart?.remove();

        // ── Price line chart ──
        this.lineChart = this.makeChart(this.lineChartRef.nativeElement, 280);
        this.lineSeries = this.lineChart.addSeries(LineSeries, {
            color: '#00d4ff', lineWidth: 2
        });
        this.lineSeries.setData(daily.map(d => ({
            time: d.tradingDate,   // 'YYYY-MM-DD' — accepted directly by lightweight-charts
            value: d.close
        })));

        // ── Volume histogram ──
        this.volChart = this.makeChart(this.volChartRef.nativeElement, 140);
        this.volSeries = this.volChart.addSeries(HistogramSeries, {
            color: 'rgba(0,212,255,0.35)',
            priceFormat: { type: 'volume' }
        });
        this.volSeries.setData(daily.map(d => ({
            time:  d.tradingDate,
            value: d.volume,
            color: (d.dailyReturn ?? 0) >= 0 ? 'rgba(74,222,128,0.45)' : 'rgba(248,113,113,0.45)'
        })));

        // Sync scroll
        this.lineChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range) this.volChart?.timeScale().setVisibleLogicalRange(range);
        });
    }

    private buildCandleChart() {
        const ohlc = this.filteredOhlc();
        if (!ohlc.length || !this.candleChartRef?.nativeElement) return;

        this.candleChart?.remove();
        this.candleChart = this.makeChart(this.candleChartRef.nativeElement, 380);
        this.candleSeries = this.candleChart.addSeries(CandlestickSeries, {
            upColor: '#4ade80', downColor: '#f87171',
            borderUpColor: '#4ade80', borderDownColor: '#f87171',
            wickUpColor: '#4ade80', wickDownColor: '#f87171'
        });

        this.candleSeries.setData(ohlc.map(o => ({
            // Convert ISO datetime → Unix seconds for intraday
            time:  Math.floor(new Date(o.windowStart).getTime() / 1000) as unknown as string,
            open:  o.open,
            high:  o.high,
            low:   o.low,
            close: o.close
        })));
    }

    // ── Formatters ──
    fmt(n: number | null | undefined, dec = 2): string {
        if (n == null) return '—';
        return n.toLocaleString('vi-VN', { minimumFractionDigits: dec, maximumFractionDigits: dec });
    }

    fmtShort(n: number | null | undefined): string {
        if (n == null) return '—';
        if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1) + ' tỷ';
        if (n >= 1_000_000)     return (n / 1_000_000).toFixed(1) + ' tr';
        if (n >= 1_000)         return (n / 1_000).toFixed(0) + 'K';
        return n.toFixed(0);
    }

    fmtTime(s: string | null | undefined): string {
        if (!s) return '—';
        return new Date(s).toLocaleString('vi-VN');
    }

    ngOnDestroy() {
        this.pollSub?.unsubscribe();
        this.ohlcPollSub?.unsubscribe();
        this.countdownSub?.unsubscribe();
        this.lineChart?.remove();
        this.volChart?.remove();
        this.candleChart?.remove();
    }
}
