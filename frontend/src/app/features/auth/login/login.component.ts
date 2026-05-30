import { Component, inject, signal, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink, Router } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { interval, Subscription } from 'rxjs';
import { AuthService } from '../../../../core/services/auth.service';

type Step = 'email' | 'otp';

@Component({
    selector: 'app-login',
    standalone: true,
    imports: [CommonModule, RouterLink, ReactiveFormsModule],
    templateUrl: './login.component.html',
    styleUrl: './login.component.css'
})
export class LoginComponent implements OnDestroy {
    private fb      = inject(FormBuilder);
    private auth    = inject(AuthService);
    private router  = inject(Router);

    step    = signal<Step>('email');
    loading = signal(false);
    error   = signal('');
    email   = signal('');

    // Countdown for OTP expiry
    countdown    = signal(0);
    otpExpired   = signal(false);
    private cdSub?: Subscription;

    emailForm: FormGroup = this.fb.group({
        email: ['', [Validators.required, Validators.email]]
    });

    otpForm: FormGroup = this.fb.group({
        otp: ['', [Validators.required, Validators.pattern(/^\d{6}$/)]]
    });

    get emailCtrl() { return this.emailForm.get('email')!; }
    get otpCtrl()   { return this.otpForm.get('otp')!; }

    // ── Step 1: submit email ──
    submitEmail() {
        if (this.emailForm.invalid) { this.emailForm.markAllAsTouched(); return; }
        this.loading.set(true);
        this.error.set('');
        const email = this.emailCtrl.value.trim().toLowerCase();

        this.auth.login({ email }).subscribe({
            next: res => {
                this.loading.set(false);
                if (res.data.next_step === 'REGISTER_REQUIRED') {
                    this.router.navigate(['/register'], { queryParams: { email } });
                    return;
                }
                this.email.set(email);
                this.startCountdown(res.data.expires_in ?? 300);
                this.step.set('otp');
            },
            error: err => {
                this.loading.set(false);
                this.error.set(err?.error?.message || 'Gửi OTP thất bại. Vui lòng thử lại.');
            }
        });
    }

    // ── Step 2: verify OTP ──
    submitOtp() {
        if (this.otpForm.invalid) { this.otpForm.markAllAsTouched(); return; }
        this.loading.set(true);
        this.error.set('');

        this.auth.verifyLoginOtp({ email: this.email(), otp: this.otpCtrl.value }).subscribe({
            next: () => { this.loading.set(false); this.router.navigate(['/dashboard']); },
            error: err => {
                this.loading.set(false);
                this.error.set(err?.error?.message || 'OTP không hợp lệ hoặc đã hết hạn.');
            }
        });
    }

    backToEmail() {
        this.step.set('email');
        this.error.set('');
        this.otpForm.reset();
        this.cdSub?.unsubscribe();
    }

    resendOtp() {
        this.otpForm.reset();
        this.error.set('');
        this.loading.set(true);
        this.auth.login({ email: this.email() }).subscribe({
            next: res => { this.loading.set(false); this.startCountdown(res.data.expires_in ?? 300); },
            error: err => { this.loading.set(false); this.error.set(err?.error?.message || 'Gửi lại OTP thất bại.'); }
        });
    }

    private startCountdown(seconds: number) {
        this.cdSub?.unsubscribe();
        this.otpExpired.set(false);
        this.countdown.set(seconds);
        this.cdSub = interval(1_000).subscribe(() => {
            this.countdown.update(n => {
                if (n <= 1) { this.otpExpired.set(true); this.cdSub?.unsubscribe(); return 0; }
                return n - 1;
            });
        });
    }

    fmtCountdown(s: number): string {
        const m = Math.floor(s / 60), sec = s % 60;
        return `${m}:${sec.toString().padStart(2, '0')}`;
    }

    ngOnDestroy() { this.cdSub?.unsubscribe(); }
}
