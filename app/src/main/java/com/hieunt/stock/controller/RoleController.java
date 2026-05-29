package com.hieunt.stock.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.repository.entity.RoleEntity;
import com.hieunt.stock.service.BaseService;
import com.hieunt.stock.service.RoleService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/admin/rbac/roles")
@RequiredArgsConstructor
public class RoleController extends BaseController<RoleEntity> {

    private final RoleService roleService;

    @Override
    protected BaseService<RoleEntity> getBaseService() {
        return roleService;
    }

    @Override
    protected String getPermissionPrefix() {
        return "role";
    }
}
