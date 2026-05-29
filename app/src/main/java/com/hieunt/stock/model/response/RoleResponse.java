package com.hieunt.stock.model.response;

import java.util.List;
import java.util.stream.Collectors;

import com.hieunt.stock.repository.entity.RoleEntity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RoleResponse {
    private Long id;
    private String name;
    private List<PermissionResponse> permissions;

    public static RoleResponse fromEntity(RoleEntity entity) {
        return RoleResponse.builder()
                .id(entity.getId())
                .name(entity.getName())
                .permissions(entity.getPermissions().stream()
                        .map(PermissionResponse::fromEntity)
                        .collect(Collectors.toList()))
                .build();
    }
}
