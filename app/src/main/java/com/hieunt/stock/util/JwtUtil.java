package com.hieunt.stock.util;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hieunt.stock.constant.TokenType;
import com.hieunt.stock.model.response.AuthResponse;
import com.hieunt.stock.repository.entity.PermissionEntity;
import com.hieunt.stock.repository.entity.RoleEntity;
import com.hieunt.stock.repository.entity.UserEntity;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

@Component
public class JwtUtil {

    @Value("${app.jwt.secret}")
    private String secret;

    @Value("${app.jwt.access-token-expiration-seconds:3600}")
    private long accessTokenExpirationSeconds;

    @Value("${app.jwt.refresh-token-expiration-seconds:604800}")
    private long refreshTokenExpirationSeconds;

    public AuthResponse generateAuthResponse(UserEntity user) {
        String accessToken = generateToken(user, TokenType.ACCESS_TOKEN, accessTokenExpirationSeconds);
        String refreshToken = generateToken(user, TokenType.REFRESH_TOKEN, refreshTokenExpirationSeconds);

        return AuthResponse.builder()
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .expiresIn(accessTokenExpirationSeconds)
                .refreshExpiresIn(refreshTokenExpirationSeconds)
                .tokenType("Bearer")
                .build();
    }

    private String generateToken(UserEntity user, TokenType tokenType, long expirationSeconds) {
        Instant now = Instant.now();
        Instant expiry = now.plusSeconds(expirationSeconds);
        List<String> roles = user.getRoles().stream()
                .map(RoleEntity::getName)
                .collect(Collectors.toList());
        Set<String> permissions = user.getRoles().stream()
                .flatMap(role -> role.getPermissions().stream())
                .map(PermissionEntity::getName)
                .collect(Collectors.toSet());

        return Jwts.builder()
                .setSubject(String.valueOf(user.getId()))
                .claim("email", user.getEmail())
                .claim("preferred_username", user.getEmail())
                .claim("roles", roles)
                .claim("permissions", permissions)
                .claim("token_type", tokenType.name())
                .setIssuedAt(Date.from(now))
                .setExpiration(Date.from(expiry))
                .signWith(Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8)), SignatureAlgorithm.HS256)
                .compact();
    }
}
