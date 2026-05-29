package com.hieunt.stock.util;

import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.hieunt.stock.exception.HIEUNTException;

public final class SecurityUtil {

    private SecurityUtil() {
    }

    public static Optional<String> getCurrentUsername() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated()) {
            return Optional.empty();
        }
        return Optional.ofNullable(authentication.getName());
    }

    public static void checkPermission(String permissionPrefix, String action) throws HIEUNTException {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String authority = permissionPrefix + ":" + action;
        boolean hasPermission = authentication != null
                && authentication.getAuthorities().stream()
                        .anyMatch(item -> authority.equals(item.getAuthority()) || "ROLE_ADMIN".equals(item.getAuthority()));

        if (!hasPermission) {
            throw new HIEUNTException(HttpStatus.FORBIDDEN, "Forbidden", "403");
        }
    }
}
