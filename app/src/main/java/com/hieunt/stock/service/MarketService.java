package com.hieunt.stock.service;

import java.util.List;

import com.hieunt.stock.model.response.MarketSectorResponse;
import com.hieunt.stock.model.response.MarketSummaryResponse;
import com.hieunt.stock.model.response.MarketTopGainerResponse;
import com.hieunt.stock.model.response.MarketTopLiquidityResponse;

public interface MarketService {
    MarketSummaryResponse getSummary();

    List<MarketTopGainerResponse> getTopGainers();

    List<MarketTopLiquidityResponse> getTopLiquidity();

    List<MarketSectorResponse> getSectors();
}
