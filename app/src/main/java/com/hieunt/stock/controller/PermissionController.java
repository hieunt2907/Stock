package com.hieunt.stock.controller;

import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.repository.entity.PermissionEntity;
import com.hieunt.stock.service.BaseService;
import com.hieunt.stock.service.PermissionService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/admin/rbac/permissions")
@RequiredArgsConstructor
public class PermissionController extends BaseController<PermissionEntity> {

    private final PermissionService permissionService;

    @Override
    protected BaseService<PermissionEntity> getBaseService() {
        return permissionService;
    }

    @Override
    protected String getPermissionPrefix() {
        return "permission";
    }

    @Override
    protected List<String> getAllowedRoles() {
        return List.of("ADMIN");
    }
}
