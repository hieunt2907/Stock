package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MarketSummaryResponse {
    private LocalDate tradingDate;
    private Long totalSymbols;
    private Long totalVolume;
    private BigDecimal totalValue;
    private BigDecimal avgReturn;
    private Long advancers;
    private Long decliners;
    private Long unchanged;
    private LocalDateTime ingestionTime;
}
