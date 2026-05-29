import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink, Router } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { AuthService } from '../../../../core/services/auth.service';

@Component({
    selector: 'app-register',
    standalone: true,
    imports: [CommonModule, RouterLink, ReactiveFormsModule],
    templateUrl: './register.component.html',
    styleUrl: './register.component.css'
})
export class RegisterComponent {
    private fb = inject(FormBuilder);
    private authService = inject(AuthService);
    private router = inject(Router);

    form: FormGroup = this.fb.group({
        email: ['', [Validators.required, Validators.email]],
        password: ['', [Validators.required, Validators.minLength(6)]],
        confirmPassword: ['', [Validators.required]]
    }, { validators: this.passwordMatchValidator });

    loading = false;
    errorMsg = '';
    showPassword = false;
    showConfirm = false;

    get email() { return this.form.get('email')!; }
    get password() { return this.form.get('password')!; }
    get confirmPassword() { return this.form.get('confirmPassword')!; }

    private passwordMatchValidator(group: FormGroup) {
        const pw = group.get('password')?.value;
        const cpw = group.get('confirmPassword')?.value;
        return pw === cpw ? null : { passwordMismatch: true };
    }

    togglePassword() { this.showPassword = !this.showPassword; }
    toggleConfirm() { this.showConfirm = !this.showConfirm; }

    onSubmit() {
        if (this.form.invalid) {
            this.form.markAllAsTouched();
            return;
        }
        this.loading = true;
        this.errorMsg = '';
        const { email, password } = this.form.value;
        this.authService.register({ email, password }).subscribe({
            next: () => {
                this.loading = false;
                this.router.navigate(['/dashboard']);
            },
            error: (err) => {
                this.loading = false;
                this.errorMsg = err?.error?.message || 'Đăng ký thất bại. Vui lòng thử lại.';
            }
        });
    }
}
