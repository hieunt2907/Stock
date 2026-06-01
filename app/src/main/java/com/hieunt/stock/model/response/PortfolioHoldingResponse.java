package com.hieunt.stock.model.response;

import java.math.BigDecimal;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PortfolioHoldingResponse {
    private String symbol;
    private BigDecimal holdingQuantity;
    private BigDecimal averageBuyPrice;
    private BigDecimal latestPrice;
    private BigDecimal costBasis;
    private BigDecimal currentValue;
    private BigDecimal profitLoss;
    private BigDecimal profitLossPercent;
}
