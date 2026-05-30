package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class StockOhlcResponse {
    private String symbol;
    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private Long volume;
    private BigDecimal value;
    private String source;
}
