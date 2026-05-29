package com.hieunt.stock.repository;

import java.util.Optional;

import com.hieunt.stock.repository.entity.PermissionEntity;

public interface PermissionRepository extends BaseRepository<PermissionEntity> {
    Optional<PermissionEntity> findByName(String name);

    boolean existsByName(String name);
}
