package com.hieunt.stock.repository;

import java.util.Optional;

import com.hieunt.stock.repository.entity.UserEntity;

public interface UserRepository extends BaseRepository<UserEntity> {
    Optional<UserEntity> findByEmail(String email);

    boolean existsByEmail(String email);
}
