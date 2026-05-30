import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { BaseResponse } from '../models/auth.model';
import { environment } from '../../environments/environment';
import {
    MarketSummaryResponse,
    MarketTopGainerResponse,
    MarketTopLiquidityResponse,
    MarketSectorResponse
} from '../models/market.model';

@Injectable({ providedIn: 'root' })
export class MarketService {
    private readonly http    = inject(HttpClient);
    private readonly API_URL = `${environment.apiUrl}/api/market`;

    getSummary(): Observable<MarketSummaryResponse> {
        return this.http.get<BaseResponse<MarketSummaryResponse>>(`${this.API_URL}/summary`).pipe(
            map(r => r.data)
        );
    }

    getTopGainers(): Observable<MarketTopGainerResponse[]> {
        return this.http.get<BaseResponse<MarketTopGainerResponse[]>>(`${this.API_URL}/top-gainers`).pipe(
            map(r => r.data)
        );
    }

    getTopLiquidity(): Observable<MarketTopLiquidityResponse[]> {
        return this.http.get<BaseResponse<MarketTopLiquidityResponse[]>>(`${this.API_URL}/top-liquidity`).pipe(
            map(r => r.data)
        );
    }

    getSectors(): Observable<MarketSectorResponse[]> {
        return this.http.get<BaseResponse<MarketSectorResponse[]>>(`${this.API_URL}/sectors`).pipe(
            map(r => r.data)
        );
    }
}
