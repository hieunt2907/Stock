package com.hieunt.stock.service;

import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.request.LoginRequest;
import com.hieunt.stock.model.request.RegisterRequest;
import com.hieunt.stock.model.request.VerifyOtpRequest;
import com.hieunt.stock.model.response.AuthFlowResponse;
import com.hieunt.stock.model.response.AuthResponse;

public interface AuthService {
    AuthFlowResponse register(RegisterRequest request) throws HIEUNTException;

    AuthFlowResponse login(LoginRequest request) throws HIEUNTException;

    AuthResponse verifyRegisterOtp(VerifyOtpRequest request) throws HIEUNTException;

    AuthResponse verifyLoginOtp(VerifyOtpRequest request) throws HIEUNTException;
}
