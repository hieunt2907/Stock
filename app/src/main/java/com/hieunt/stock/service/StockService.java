package com.hieunt.stock.service;

import java.util.List;

import com.hieunt.stock.model.response.StockResponse;

public interface StockService {
    List<StockResponse> findStocks();
}
