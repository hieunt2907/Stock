package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MarketTopLiquidityResponse {
    private LocalDate tradingDate;
    private String symbol;
    private String companyName;
    private String sector;
    private Long volume;
    private BigDecimal value;
    private Long rank;
    private LocalDateTime ingestionTime;
}
