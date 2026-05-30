import { Component, inject, signal, computed, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { forkJoin } from 'rxjs';
import { MarketService } from '../../../core/services/market.service';
import { TopbarComponent } from '../../shared/topbar/topbar.component';
import {
    MarketSummaryResponse,
    MarketTopGainerResponse,
    MarketTopLiquidityResponse,
    MarketSectorResponse
} from '../../../core/models/market.model';

@Component({
    selector: 'app-dashboard',
    standalone: true,
    imports: [CommonModule, RouterLink, TopbarComponent],
    templateUrl: './dashboard.component.html',
    styleUrl: './dashboard.component.css'
})
export class DashboardComponent implements OnInit {
    private marketSvc = inject(MarketService);
    Math = Math;

    loading = signal(true);
    error   = signal('');

    summary   = signal<MarketSummaryResponse | null>(null);
    gainers   = signal<MarketTopGainerResponse[]>([]);
    liquidity = signal<MarketTopLiquidityResponse[]>([]);
    sectors   = signal<MarketSectorResponse[]>([]);

    sortedSectors = computed(() =>
        [...this.sectors()].sort((a, b) => (b.avgReturn ?? 0) - (a.avgReturn ?? 0))
    );

    ngOnInit() {
        forkJoin({
            summary:   this.marketSvc.getSummary(),
            gainers:   this.marketSvc.getTopGainers(),
            liquidity: this.marketSvc.getTopLiquidity(),
            sectors:   this.marketSvc.getSectors()
        }).subscribe({
            next: ({ summary, gainers, liquidity, sectors }) => {
                this.summary.set(summary);
                this.gainers.set(gainers);
                this.liquidity.set(liquidity);
                this.sectors.set(sectors);
                this.loading.set(false);
            },
            error: () => {
                this.error.set('Không thể tải dữ liệu thị trường.');
                this.loading.set(false);
            }
        });
    }

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

    advancerPct = computed(() => {
        const s = this.summary();
        if (!s || !s.totalSymbols) return 0;
        return (s.advancers / s.totalSymbols) * 100;
    });

    declinerPct = computed(() => {
        const s = this.summary();
        if (!s || !s.totalSymbols) return 0;
        return (s.decliners / s.totalSymbols) * 100;
    });
}
