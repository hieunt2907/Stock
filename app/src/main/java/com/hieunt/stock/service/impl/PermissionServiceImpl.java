package com.hieunt.stock.service.impl;

import org.springframework.stereotype.Service;

import com.hieunt.stock.mapper.BaseMapper;
import com.hieunt.stock.mapper.PermissionMapper;
import com.hieunt.stock.repository.BaseRepository;
import com.hieunt.stock.repository.PermissionRepository;
import com.hieunt.stock.repository.entity.PermissionEntity;
import com.hieunt.stock.service.PermissionService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PermissionServiceImpl extends BaseServiceImpl<PermissionEntity> implements PermissionService {

    private final PermissionRepository permissionRepository;
    private final PermissionMapper permissionMapper;

    @Override
    protected BaseRepository<PermissionEntity> getBaseRepository() {
        return permissionRepository;
    }

    @Override
    protected BaseMapper<PermissionEntity> getBaseMapper() {
        return permissionMapper;
    }
}
