import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { BaseResponse } from '../models/auth.model';
import { environment } from '../../environments/environment';
import { StockOhlcResponse, StockResponse, StockTickResponse } from '../models/stock.model';

@Injectable({ providedIn: 'root' })
export class StockService {
    private readonly http    = inject(HttpClient);
    private readonly API_URL = `${environment.apiUrl}/api/stocks`;

    findStocks(): Observable<StockResponse[]> {
        return this.http.get<BaseResponse<StockResponse[]>>(this.API_URL).pipe(map(r => r.data));
    }

    findStock(symbol: string): Observable<StockResponse> {
        return this.http.get<BaseResponse<StockResponse>>(`${this.API_URL}/${symbol}`).pipe(map(r => r.data));
    }

    findDaily(symbol: string): Observable<StockResponse[]> {
        return this.http.get<BaseResponse<StockResponse[]>>(`${this.API_URL}/${symbol}/daily`).pipe(map(r => r.data));
    }

    findLatest(symbol: string): Observable<StockTickResponse> {
        return this.http.get<BaseResponse<StockTickResponse>>(`${this.API_URL}/${symbol}/latest`).pipe(map(r => r.data));
    }

    findLatestBatch(symbols: string[]): Observable<StockTickResponse[]> {
        const q = symbols.join(',');
        return this.http.get<BaseResponse<StockTickResponse[]>>(`${this.API_URL}/latest`, { params: { symbols: q } }).pipe(map(r => r.data));
    }

    findOhlc(symbol: string): Observable<StockOhlcResponse[]> {
        return this.http.get<BaseResponse<StockOhlcResponse[]>>(`${this.API_URL}/${symbol}/ohlc`).pipe(map(r => r.data));
    }
}
