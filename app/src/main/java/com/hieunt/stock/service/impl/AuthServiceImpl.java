package com.hieunt.stock.service.impl;

import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.hieunt.stock.constant.Status;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.request.LoginRequest;
import com.hieunt.stock.model.request.RegisterRequest;
import com.hieunt.stock.model.response.AuthResponse;
import com.hieunt.stock.repository.RoleRepository;
import com.hieunt.stock.repository.UserRepository;
import com.hieunt.stock.repository.entity.RoleEntity;
import com.hieunt.stock.repository.entity.UserEntity;
import com.hieunt.stock.service.AuthService;
import com.hieunt.stock.util.JwtUtil;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {

    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;

    @Override
    @Transactional
    public AuthResponse register(RegisterRequest request) throws HIEUNTException {
        String email = normalizeEmail(request.getEmail());
        if (userRepository.existsByEmail(email)) {
            throw new HIEUNTException(HttpStatus.CONFLICT, "Email already exists", "AUTH_001");
        }

        UserEntity user = new UserEntity();
        user.setEmail(email);
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setStatus(Status.ACTIVE);
        user.getRoles().add(getOrCreateDefaultUserRole());
        user = userRepository.save(user);

        return jwtUtil.generateAuthResponse(user);
    }

    @Override
    @Transactional(readOnly = true)
    public AuthResponse login(LoginRequest request) throws HIEUNTException {
        String email = normalizeEmail(request.getEmail());
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new HIEUNTException(HttpStatus.UNAUTHORIZED, "Invalid email or password", "AUTH_002"));

        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            throw new HIEUNTException(HttpStatus.UNAUTHORIZED, "Invalid email or password", "AUTH_002");
        }

        return jwtUtil.generateAuthResponse(user);
    }

    private String normalizeEmail(String email) {
        return email.trim().toLowerCase();
    }

    private RoleEntity getOrCreateDefaultUserRole() {
        return roleRepository.findByName("USER")
                .orElseGet(() -> roleRepository.save(new RoleEntity("USER")));
    }
}
