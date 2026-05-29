package com.hieunt.stock.mapper;

import org.mapstruct.Mapper;

import com.hieunt.stock.repository.entity.PermissionEntity;

@Mapper(componentModel = "spring")
public interface PermissionMapper extends BaseMapper<PermissionEntity> {
}
