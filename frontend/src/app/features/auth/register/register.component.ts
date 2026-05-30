import { Component, inject, signal, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink, Router, ActivatedRoute } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators, AbstractControlOptions } from '@angular/forms';
import { interval, Subscription } from 'rxjs';
import { AuthService } from '../../../../core/services/auth.service';

type Step = 'form' | 'otp';

@Component({
    selector: 'app-register',
    standalone: true,
    imports: [CommonModule, RouterLink, ReactiveFormsModule],
    templateUrl: './register.component.html',
    styleUrl: './register.component.css'
})
export class RegisterComponent implements OnDestroy {
    private fb     = inject(FormBuilder);
    private auth   = inject(AuthService);
    private router = inject(Router);
    private route  = inject(ActivatedRoute);

    step    = signal<Step>('form');
    loading = signal(false);
    error   = signal('');
    email   = signal('');

    countdown  = signal(0);
    otpExpired = signal(false);
    private cdSub?: Subscription;

    showPassword = false;
    showConfirm  = false;

    registerForm: FormGroup = this.fb.group({
        email:           [this.route.snapshot.queryParamMap.get('email') ?? '', [Validators.required, Validators.email]],
        password:        ['', [Validators.required, Validators.minLength(6)]],
        confirmPassword: ['', [Validators.required]]
    }, { validators: this.matchPasswords } as AbstractControlOptions);

    otpForm: FormGroup = this.fb.group({
        otp: ['', [Validators.required, Validators.pattern(/^\d{6}$/)]]
    });

    get emailCtrl()   { return this.registerForm.get('email')!; }
    get passwordCtrl(){ return this.registerForm.get('password')!; }
    get confirmCtrl() { return this.registerForm.get('confirmPassword')!; }
    get otpCtrl()     { return this.otpForm.get('otp')!; }

    private matchPasswords(g: FormGroup) {
        return g.get('password')?.value === g.get('confirmPassword')?.value ? null : { mismatch: true };
    }

    // ── Step 1: submit register form ──
    submitForm() {
        if (this.registerForm.invalid) { this.registerForm.markAllAsTouched(); return; }
        this.loading.set(true);
        this.error.set('');
        const { email, password } = this.registerForm.value;

        this.auth.register({ email: email.trim().toLowerCase(), password }).subscribe({
            next: res => {
                this.loading.set(false);
                this.email.set(email.trim().toLowerCase());
                this.startCountdown(res.data.expires_in ?? 300);
                this.step.set('otp');
            },
            error: err => {
                this.loading.set(false);
                this.error.set(err?.error?.message || 'Đăng ký thất bại. Vui lòng thử lại.');
            }
        });
    }

    // ── Step 2: verify OTP ──
    submitOtp() {
        if (this.otpForm.invalid) { this.otpForm.markAllAsTouched(); return; }
        this.loading.set(true);
        this.error.set('');

        this.auth.verifyRegisterOtp({ email: this.email(), otp: this.otpCtrl.value }).subscribe({
            next: () => { this.loading.set(false); this.router.navigate(['/dashboard']); },
            error: err => {
                this.loading.set(false);
                this.error.set(err?.error?.message || 'OTP không hợp lệ hoặc đã hết hạn.');
            }
        });
    }

    backToForm() {
        this.step.set('form');
        this.error.set('');
        this.otpForm.reset();
        this.cdSub?.unsubscribe();
    }

    resendOtp() {
        this.otpForm.reset();
        this.error.set('');
        this.loading.set(true);
        const { email, password } = this.registerForm.value;
        this.auth.register({ email: email.trim().toLowerCase(), password }).subscribe({
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

    togglePassword() { this.showPassword = !this.showPassword; }
    toggleConfirm()  { this.showConfirm  = !this.showConfirm;  }

    ngOnDestroy() { this.cdSub?.unsubscribe(); }
}
