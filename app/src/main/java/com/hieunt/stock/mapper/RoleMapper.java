package com.hieunt.stock.mapper;

import org.mapstruct.Mapper;

import com.hieunt.stock.repository.entity.RoleEntity;

@Mapper(componentModel = "spring")
public interface RoleMapper extends BaseMapper<RoleEntity> {
}
