package com.hieunt.stock.controller;

import javax.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.request.LoginRequest;
import com.hieunt.stock.model.request.RegisterRequest;
import com.hieunt.stock.model.response.AuthResponse;
import com.hieunt.stock.service.AuthService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/public/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/register")
    public ResponseEntity<BaseResponse<AuthResponse>> register(@Valid @RequestBody RegisterRequest request)
            throws HIEUNTException {
        AuthResponse authResponse = authService.register(request);
        return ResponseEntity.ok(BaseResponse.<AuthResponse>builder()
                .code(HttpStatus.CREATED)
                .message(SuccessMessageKey.CREATE)
                .data(authResponse)
                .build());
    }

    @PostMapping("/login")
    public ResponseEntity<BaseResponse<AuthResponse>> login(@Valid @RequestBody LoginRequest request)
            throws HIEUNTException {
        AuthResponse authResponse = authService.login(request);
        return ResponseEntity.ok(BaseResponse.<AuthResponse>builder()
                .code(HttpStatus.OK)
                .message(SuccessMessageKey.SUCCESS)
                .data(authResponse)
                .build());
    }
}
