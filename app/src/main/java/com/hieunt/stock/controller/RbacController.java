package com.hieunt.stock.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.request.CreateUserAccessRequest;
import com.hieunt.stock.model.response.RoleResponse;
import com.hieunt.stock.model.response.UserRoleResponse;
import com.hieunt.stock.service.RbacService;
import com.hieunt.stock.util.SecurityUtil;

import javax.validation.Valid;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/admin/rbac")
@RequiredArgsConstructor
public class RbacController {

    private final RbacService rbacService;

    @PostMapping("/users")
    public ResponseEntity<BaseResponse<UserRoleResponse>> createUserWithAccess(
            @Valid @org.springframework.web.bind.annotation.RequestBody CreateUserAccessRequest request)
            throws HIEUNTException {
        checkAccess("user", "create");
        return ResponseEntity.ok(success(HttpStatus.CREATED, rbacService.createUserWithAccess(request)));
    }

    @PostMapping("/roles/{roleId}/permissions/{permissionId}")
    public ResponseEntity<BaseResponse<RoleResponse>> addPermissionToRole(
            @PathVariable Long roleId,
            @PathVariable Long permissionId) throws HIEUNTException {
        checkAccess("role", "update");
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.addPermissionToRole(roleId, permissionId)));
    }

    @DeleteMapping("/roles/{roleId}/permissions/{permissionId}")
    public ResponseEntity<BaseResponse<RoleResponse>> removePermissionFromRole(
            @PathVariable Long roleId,
            @PathVariable Long permissionId) throws HIEUNTException {
        checkAccess("role", "update");
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.removePermissionFromRole(roleId, permissionId)));
    }

    @PostMapping("/users/{userId}/roles/{roleId}")
    public ResponseEntity<BaseResponse<UserRoleResponse>> addRoleToUser(
            @PathVariable Long userId,
            @PathVariable Long roleId) throws HIEUNTException {
        checkAccess("user", "update");
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.addRoleToUser(userId, roleId)));
    }

    @DeleteMapping("/users/{userId}/roles/{roleId}")
    public ResponseEntity<BaseResponse<UserRoleResponse>> removeRoleFromUser(
            @PathVariable Long userId,
            @PathVariable Long roleId) throws HIEUNTException {
        checkAccess("user", "update");
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.removeRoleFromUser(userId, roleId)));
    }

    @GetMapping("/users/{userId}/roles")
    public ResponseEntity<BaseResponse<UserRoleResponse>> getUserRoles(@PathVariable Long userId)
            throws HIEUNTException {
        checkAccess("user", "read");
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.getUserRoles(userId)));
    }

    private void checkAccess(String prefix, String action) {
        SecurityUtil.checkAnyRole(List.of("ADMIN"));
        SecurityUtil.checkPermission(prefix, action);
    }

    private <T> BaseResponse<T> success(HttpStatus status, T data) {
        return BaseResponse.<T>builder()
                .code(status)
                .message(SuccessMessageKey.SUCCESS)
                .data(data)
                .build();
    }
}
