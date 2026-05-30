package com.hieunt.stock.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.hieunt.stock.constant.Status;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.request.CreateUserAccessRequest;
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
    private final PasswordEncoder passwordEncoder;

    @Override
    @Transactional
    public UserRoleResponse createUserWithAccess(CreateUserAccessRequest request) throws HIEUNTException {
        String email = normalizeEmail(request.getEmail());
        if (userRepository.existsByEmail(email)) {
            throw new HIEUNTException(HttpStatus.CONFLICT, "Email already exists", "AUTH_001");
        }

        List<String> roleNames = normalizeRoles(request.getRoles());
        if (roleNames.isEmpty()) {
            roleNames = List.of("USER");
        }
        List<String> permissionNames = normalizePermissions(request.getPermissions());

        List<PermissionEntity> permissions = permissionNames.stream()
                .map(this::getOrCreatePermission)
                .collect(Collectors.toList());

        List<RoleEntity> roles = roleNames.stream()
                .map(roleName -> getOrCreateRole(roleName, permissions))
                .collect(Collectors.toList());

        UserEntity user = new UserEntity();
        user.setEmail(email);
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setStatus(Status.ACTIVE);
        user.getRoles().addAll(roles);

        return UserRoleResponse.fromEntity(userRepository.save(user));
    }

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

    private RoleEntity getOrCreateRole(String roleName, List<PermissionEntity> permissions) {
        RoleEntity role = roleRepository.findByName(roleName)
                .orElseGet(() -> roleRepository.save(new RoleEntity(roleName)));
        role.getPermissions().addAll(permissions);
        return roleRepository.save(role);
    }

    private PermissionEntity getOrCreatePermission(String permissionName) {
        return permissionRepository.findByName(permissionName)
                .orElseGet(() -> permissionRepository.save(new PermissionEntity(permissionName, null)));
    }

    private List<String> normalizeRoles(Collection<String> roles) {
        if (roles == null) {
            return List.of();
        }
        return roles.stream()
                .filter(item -> item != null && !item.isBlank())
                .map(item -> item.trim().toUpperCase())
                .distinct()
                .collect(Collectors.toList());
    }

    private List<String> normalizePermissions(Collection<String> permissions) {
        if (permissions == null) {
            return List.of();
        }
        return permissions.stream()
                .filter(item -> item != null && !item.isBlank())
                .map(item -> item.trim().toLowerCase())
                .distinct()
                .collect(Collectors.toList());
    }

    private String normalizeEmail(String email) {
        return email.trim().toLowerCase();
    }

}
