import { HttpClient } from '@angular/common/http';
import { Injectable, inject, signal } from '@angular/core';
import { Router } from '@angular/router';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';
import {
    AuthFlowResponse, AuthResponse, BaseResponse,
    LoginRequest, RegisterRequest, VerifyOtpRequest
} from '../models/auth.model';
import { environment } from '../../environments/environment';

export interface UserInfo {
    username: string;
    email?: string;
}

@Injectable({ providedIn: 'root' })
export class AuthService {
    private readonly http   = inject(HttpClient);
    private readonly router = inject(Router);

    private readonly TOKEN_KEY         = 'access_token';
    private readonly REFRESH_TOKEN_KEY = 'refresh_token';
    private readonly API_URL           = `${environment.apiUrl}/public/auth`;

    currentUser = signal<UserInfo | null>(null);

    constructor() {
        this.restoreUserFromToken();
    }

    /** Step 1 — send OTP to email */
    login(request: LoginRequest): Observable<BaseResponse<AuthFlowResponse>> {
        return this.http.post<BaseResponse<AuthFlowResponse>>(`${this.API_URL}/login`, request);
    }

    /** Step 1 — send OTP + store hashed password */
    register(request: RegisterRequest): Observable<BaseResponse<AuthFlowResponse>> {
        return this.http.post<BaseResponse<AuthFlowResponse>>(`${this.API_URL}/register`, request);
    }

    /** Step 2 — verify OTP, get JWT */
    verifyLoginOtp(request: VerifyOtpRequest): Observable<BaseResponse<AuthResponse>> {
        return this.http.post<BaseResponse<AuthResponse>>(`${this.API_URL}/login/verify-otp`, request).pipe(
            tap(res => this.handleAuthSuccess(res.data))
        );
    }

    /** Step 2 — verify OTP, create account, get JWT */
    verifyRegisterOtp(request: VerifyOtpRequest): Observable<BaseResponse<AuthResponse>> {
        return this.http.post<BaseResponse<AuthResponse>>(`${this.API_URL}/register/verify-otp`, request).pipe(
            tap(res => this.handleAuthSuccess(res.data))
        );
    }

    logout(): void {
        localStorage.removeItem(this.TOKEN_KEY);
        localStorage.removeItem(this.REFRESH_TOKEN_KEY);
        this.currentUser.set(null);
        this.router.navigate(['/login']);
    }

    getToken(): string | null {
        return localStorage.getItem(this.TOKEN_KEY);
    }

    isAuthenticated(): boolean {
        return !!this.getToken();
    }

    private handleAuthSuccess(auth: AuthResponse): void {
        if (!auth) return;
        localStorage.setItem(this.TOKEN_KEY, auth.access_token);
        if (auth.refresh_token) localStorage.setItem(this.REFRESH_TOKEN_KEY, auth.refresh_token);
        this.restoreUserFromToken();
    }

    private restoreUserFromToken(): void {
        const token = this.getToken();
        if (!token) { this.currentUser.set(null); return; }
        try {
            const payload = this.decodeJwt(token);
            if (payload.exp && payload.exp * 1000 < Date.now()) {
                localStorage.removeItem(this.TOKEN_KEY);
                localStorage.removeItem(this.REFRESH_TOKEN_KEY);
                this.currentUser.set(null);
                return;
            }
            this.currentUser.set({
                username: payload.email || payload.preferred_username || payload.sub || 'User',
                email:    payload.email
            });
        } catch {
            this.currentUser.set(null);
        }
    }

    private decodeJwt(token: string): any {
        const parts = token.split('.');
        if (parts.length !== 3) throw new Error('Invalid JWT');
        return JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')));
    }
}
