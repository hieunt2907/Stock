package com.hieunt.stock.service.impl;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.response.RoleResponse;
import com.hieunt.stock.model.response.UserRoleResponse;
import com.hieunt.stock.repository.PermissionRepository;
import com.hieunt.stock.repository.RoleRepository;
import com.hieunt.stock.repository.UserRepository;
import com.hieunt.stock.repository.entity.PermissionEntity;
import com.hieunt.stock.repository.entity.RoleEntity;
import com.hieunt.stock.repository.entity.UserEntity;
import com.hieunt.stock.service.RbacService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class RbacServiceImpl implements RbacService {

    private final RoleRepository roleRepository;
    private final PermissionRepository permissionRepository;
    private final UserRepository userRepository;

    @Override
    @Transactional
    public RoleResponse addPermissionToRole(Long roleId, Long permissionId) throws HIEUNTException {
        RoleEntity role = findRole(roleId);
        PermissionEntity permission = findPermission(permissionId);
        role.getPermissions().add(permission);
        return RoleResponse.fromEntity(roleRepository.save(role));
    }

    @Override
    @Transactional
    public RoleResponse removePermissionFromRole(Long roleId, Long permissionId) throws HIEUNTException {
        RoleEntity role = findRole(roleId);
        PermissionEntity permission = findPermission(permissionId);
        role.getPermissions().remove(permission);
        return RoleResponse.fromEntity(roleRepository.save(role));
    }

    @Override
    @Transactional
    public UserRoleResponse addRoleToUser(Long userId, Long roleId) throws HIEUNTException {
        UserEntity user = findUser(userId);
        RoleEntity role = findRole(roleId);
        user.getRoles().add(role);
        return UserRoleResponse.fromEntity(userRepository.save(user));
    }

    @Override
    @Transactional
    public UserRoleResponse removeRoleFromUser(Long userId, Long roleId) throws HIEUNTException {
        UserEntity user = findUser(userId);
        RoleEntity role = findRole(roleId);
        user.getRoles().remove(role);
        return UserRoleResponse.fromEntity(userRepository.save(user));
    }

    @Override
    @Transactional(readOnly = true)
    public UserRoleResponse getUserRoles(Long userId) throws HIEUNTException {
        return UserRoleResponse.fromEntity(findUser(userId));
    }

    private RoleEntity findRole(Long roleId) throws HIEUNTException {
        return roleRepository.findById(roleId)
                .orElseThrow(() -> new HIEUNTException(HttpStatus.NOT_FOUND, "Role not found", "RBAC_003"));
    }

    private PermissionEntity findPermission(Long permissionId) throws HIEUNTException {
        return permissionRepository.findById(permissionId)
                .orElseThrow(() -> new HIEUNTException(HttpStatus.NOT_FOUND, "Permission not found", "RBAC_004"));
    }

    private UserEntity findUser(Long userId) throws HIEUNTException {
        return userRepository.findById(userId)
                .orElseThrow(() -> new HIEUNTException(HttpStatus.NOT_FOUND, "User not found", "RBAC_005"));
    }

}
