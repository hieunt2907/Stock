package com.hieunt.stock.service;

import java.util.List;

import com.hieunt.stock.model.response.StockOhlcResponse;
import com.hieunt.stock.model.response.StockResponse;
import com.hieunt.stock.model.response.StockTickResponse;

public interface StockService {
    List<StockResponse> findStocks();

    StockResponse findStock(String symbol);

    List<StockResponse> findDaily(String symbol);

    StockTickResponse findLatest(String symbol);

    List<StockTickResponse> findLatestBatch(String symbols);

    List<StockOhlcResponse> findOhlc(String symbol);
}
