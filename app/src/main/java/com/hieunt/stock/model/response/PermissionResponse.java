package com.hieunt.stock.model.response;

import com.hieunt.stock.repository.entity.PermissionEntity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PermissionResponse {
    private Long id;
    private String name;
    private String description;

    public static PermissionResponse fromEntity(PermissionEntity entity) {
        return PermissionResponse.builder()
                .id(entity.getId())
                .name(entity.getName())
                .description(entity.getDescription())
                .build();
    }
}
