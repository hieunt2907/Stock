package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class StockTickResponse {
    private String eventId;
    private String symbol;
    private LocalDateTime eventTime;
    private BigDecimal price;
    private Long volume;
    private String source;
}
