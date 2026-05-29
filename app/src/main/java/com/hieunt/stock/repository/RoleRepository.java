package com.hieunt.stock.repository;

import java.util.Optional;

import com.hieunt.stock.repository.entity.RoleEntity;

public interface RoleRepository extends BaseRepository<RoleEntity> {
    Optional<RoleEntity> findByName(String name);

    boolean existsByName(String name);
}
