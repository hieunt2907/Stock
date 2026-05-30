package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MarketTopGainerResponse {
    private LocalDate tradingDate;
    private String symbol;
    private String companyName;
    private String sector;
    private BigDecimal close;
    private BigDecimal dailyReturn;
    private Long rank;
    private LocalDateTime ingestionTime;
}
