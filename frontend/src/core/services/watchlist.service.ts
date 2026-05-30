import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { BaseResponse } from '../models/auth.model';
import { WatchlistResponse } from '../models/watchlist.model';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class WatchlistService {
    private readonly http    = inject(HttpClient);
    private readonly API_URL = `${environment.apiUrl}/api/watchlists`;

    getWatchlist(): Observable<WatchlistResponse[]> {
        return this.http.get<BaseResponse<WatchlistResponse[]>>(this.API_URL).pipe(map(r => r.data));
    }

    addSymbol(symbol: string): Observable<WatchlistResponse[]> {
        return this.http.post<BaseResponse<WatchlistResponse[]>>(`${this.API_URL}/${symbol}`, {}).pipe(map(r => r.data));
    }

    removeSymbol(symbol: string): Observable<WatchlistResponse[]> {
        return this.http.delete<BaseResponse<WatchlistResponse[]>>(`${this.API_URL}/${symbol}`).pipe(map(r => r.data));
    }
}
