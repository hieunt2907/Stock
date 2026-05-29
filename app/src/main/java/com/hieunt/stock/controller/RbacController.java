package com.hieunt.stock.controller;

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
import com.hieunt.stock.model.response.RoleResponse;
import com.hieunt.stock.model.response.UserRoleResponse;
import com.hieunt.stock.service.RbacService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/admin/rbac")
@RequiredArgsConstructor
public class RbacController {

    private final RbacService rbacService;

    @PostMapping("/roles/{roleId}/permissions/{permissionId}")
    public ResponseEntity<BaseResponse<RoleResponse>> addPermissionToRole(
            @PathVariable Long roleId,
            @PathVariable Long permissionId) throws HIEUNTException {
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.addPermissionToRole(roleId, permissionId)));
    }

    @DeleteMapping("/roles/{roleId}/permissions/{permissionId}")
    public ResponseEntity<BaseResponse<RoleResponse>> removePermissionFromRole(
            @PathVariable Long roleId,
            @PathVariable Long permissionId) throws HIEUNTException {
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.removePermissionFromRole(roleId, permissionId)));
    }

    @PostMapping("/users/{userId}/roles/{roleId}")
    public ResponseEntity<BaseResponse<UserRoleResponse>> addRoleToUser(
            @PathVariable Long userId,
            @PathVariable Long roleId) throws HIEUNTException {
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.addRoleToUser(userId, roleId)));
    }

    @DeleteMapping("/users/{userId}/roles/{roleId}")
    public ResponseEntity<BaseResponse<UserRoleResponse>> removeRoleFromUser(
            @PathVariable Long userId,
            @PathVariable Long roleId) throws HIEUNTException {
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.removeRoleFromUser(userId, roleId)));
    }

    @GetMapping("/users/{userId}/roles")
    public ResponseEntity<BaseResponse<UserRoleResponse>> getUserRoles(@PathVariable Long userId)
            throws HIEUNTException {
        return ResponseEntity.ok(success(HttpStatus.OK, rbacService.getUserRoles(userId)));
    }

    private <T> BaseResponse<T> success(HttpStatus status, T data) {
        return BaseResponse.<T>builder()
                .code(status)
                .message(SuccessMessageKey.SUCCESS)
                .data(data)
                .build();
    }
}
