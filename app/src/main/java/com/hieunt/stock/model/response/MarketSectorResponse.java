package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MarketSectorResponse {
    private LocalDate tradingDate;
    private String sector;
    private Long totalSymbols;
    private Long totalVolume;
    private BigDecimal totalValue;
    private BigDecimal avgReturn;
    private String bestSymbol;
    private String worstSymbol;
    private LocalDateTime ingestionTime;
}
