package com.hieunt.stock.model.response;

import java.util.List;
import java.util.stream.Collectors;

import com.hieunt.stock.repository.entity.UserEntity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserRoleResponse {
    private Long id;
    private String email;
    private List<RoleResponse> roles;

    public static UserRoleResponse fromEntity(UserEntity entity) {
        return UserRoleResponse.builder()
                .id(entity.getId())
                .email(entity.getEmail())
                .roles(entity.getRoles().stream()
                        .map(RoleResponse::fromEntity)
                        .collect(Collectors.toList()))
                .build();
    }
}
