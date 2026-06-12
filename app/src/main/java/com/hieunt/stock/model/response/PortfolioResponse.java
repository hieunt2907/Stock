package com.hieunt.stock.model.response;

import java.time.OffsetDateTime;

import com.hieunt.stock.repository.entity.PortfolioEntity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PortfolioResponse {
    private Long id;
    private String name;
    private String createdBy;
    private OffsetDateTime createdAt;

    public static PortfolioResponse fromEntity(PortfolioEntity entity) {
        return PortfolioResponse.builder()
                .id(entity.getId())
                .name(entity.getName())
                .createdBy(entity.getCreatedBy())
                .createdAt(entity.getCreatedAt())
                .build();
    }
}
