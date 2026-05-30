package com.hieunt.stock.util;

import java.util.List;
import java.util.Optional;

import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

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

    public static Long getCurrentUserId() {
        return getCurrentUsername()
                .map(Long::valueOf)
                .orElseThrow(() -> new AccessDeniedException("Unauthenticated"));
    }

    public static void checkAnyRole(List<String> roles) {
        if (roles == null || roles.isEmpty()) {
            return;
        }

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        boolean allowed = authentication != null
                && authentication.getAuthorities()
                        .stream()
                        .map(GrantedAuthority::getAuthority)
                        .anyMatch(authority -> roles.stream()
                                .anyMatch(role -> authority.equals("ROLE_" + role)));

        if (!allowed) {
            throw new AccessDeniedException("Missing role: " + roles);
        }
    }

    public static void checkPermission(String permissionPrefix, String action) {
        String requiredPermission = permissionPrefix + ":" + action;
        String wildcardPermission = permissionPrefix + ":*";
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        boolean allowed = authentication != null
                && authentication.getAuthorities()
                        .stream()
                        .map(GrantedAuthority::getAuthority)
                        .anyMatch(authority -> authority.equals("ROLE_ADMIN")
                                || authority.equals("*:*")
                                || authority.equals(wildcardPermission)
                                || authority.equals(requiredPermission));

        if (!allowed) {
            throw new AccessDeniedException("Missing permission: " + requiredPermission);
        }
    }
}
