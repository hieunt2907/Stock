package com.hieunt.stock.model.response;

import java.math.BigDecimal;
import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PortfolioPerformanceResponse {
    private Long portfolioId;
    private BigDecimal totalCostBasis;
    private BigDecimal totalCurrentValue;
    private BigDecimal totalProfitLoss;
    private BigDecimal totalProfitLossPercent;
    private List<PortfolioHoldingResponse> holdings;
}
