package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDate;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class StockResponse {
    private String symbol;
    private String companyName;
    private String exchange;
    private String industry;
    private String sector;
    private LocalDate tradingDate;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private Long volume;
    private BigDecimal value;
    private BigDecimal dailyReturn;
    private BigDecimal volatility;
    private String source;
}
