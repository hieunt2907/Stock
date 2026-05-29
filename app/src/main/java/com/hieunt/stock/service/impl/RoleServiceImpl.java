package com.hieunt.stock.service.impl;

import org.springframework.stereotype.Service;

import com.hieunt.stock.mapper.BaseMapper;
import com.hieunt.stock.mapper.RoleMapper;
import com.hieunt.stock.repository.BaseRepository;
import com.hieunt.stock.repository.RoleRepository;
import com.hieunt.stock.repository.entity.RoleEntity;
import com.hieunt.stock.service.RoleService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class RoleServiceImpl extends BaseServiceImpl<RoleEntity> implements RoleService {

    private final RoleRepository roleRepository;
    private final RoleMapper roleMapper;

    @Override
    protected BaseRepository<RoleEntity> getBaseRepository() {
        return roleRepository;
    }

    @Override
    protected BaseMapper<RoleEntity> getBaseMapper() {
        return roleMapper;
    }
}
