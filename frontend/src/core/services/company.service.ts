import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { BaseResponse } from '../models/auth.model';
import { CompanyResponse } from '../models/company.model';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class CompanyService {
    private readonly http    = inject(HttpClient);
    private readonly API_URL = `${environment.apiUrl}/api/companies`;

    findCompanies(): Observable<CompanyResponse[]> {
        return this.http.get<BaseResponse<CompanyResponse[]>>(this.API_URL).pipe(map(r => r.data));
    }

    findCompany(symbol: string): Observable<CompanyResponse> {
        return this.http.get<BaseResponse<CompanyResponse>>(`${this.API_URL}/${symbol}`).pipe(map(r => r.data));
    }
}
