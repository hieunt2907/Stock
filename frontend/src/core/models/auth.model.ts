export interface AuthResponse {
    access_token: string;
    refresh_token: string;
    expires_in: number;
    refresh_expires_in: number;
    token_type: string;
}

export interface AuthFlowResponse {
    email: string;
    registered: boolean;
    otp_sent: boolean;
    next_step: 'VERIFY_LOGIN_OTP' | 'VERIFY_REGISTER_OTP' | 'REGISTER_REQUIRED';
    expires_in: number;
}

export interface LoginRequest {
    email: string;
}

export interface RegisterRequest {
    email: string;
    password: string;
}

export interface VerifyOtpRequest {
    email: string;
    otp: string;
}

export interface BaseResponse<T> {
    code: string | number;
    message: string;
    data: T;
}
