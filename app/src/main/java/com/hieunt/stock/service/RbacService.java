package com.hieunt.stock.service;

import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.request.CreateUserAccessRequest;
import com.hieunt.stock.model.response.RoleResponse;
import com.hieunt.stock.model.response.UserRoleResponse;

public interface RbacService {
    UserRoleResponse createUserWithAccess(CreateUserAccessRequest request) throws HIEUNTException;

    RoleResponse addPermissionToRole(Long roleId, Long permissionId) throws HIEUNTException;

    RoleResponse removePermissionFromRole(Long roleId, Long permissionId) throws HIEUNTException;

    UserRoleResponse addRoleToUser(Long userId, Long roleId) throws HIEUNTException;

    UserRoleResponse removeRoleFromUser(Long userId, Long roleId) throws HIEUNTException;

    UserRoleResponse getUserRoles(Long userId) throws HIEUNTException;
}
