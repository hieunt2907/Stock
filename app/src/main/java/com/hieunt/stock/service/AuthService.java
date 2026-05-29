package com.hieunt.stock.service;

import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.request.LoginRequest;
import com.hieunt.stock.model.request.RegisterRequest;
import com.hieunt.stock.model.response.AuthResponse;

public interface AuthService {
    AuthResponse register(RegisterRequest request) throws HIEUNTException;

    AuthResponse login(LoginRequest request) throws HIEUNTException;
}
