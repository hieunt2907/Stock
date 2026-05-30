package com.hieunt.stock.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.response.StockOhlcResponse;
import com.hieunt.stock.model.response.StockResponse;
import com.hieunt.stock.model.response.StockTickResponse;
import com.hieunt.stock.service.StockService;
import com.hieunt.stock.util.SecurityUtil;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockController {

    private final StockService stockService;

    @GetMapping
    public ResponseEntity<BaseResponse<List<StockResponse>>> findStocks() {
        checkAccess("read");
        return ResponseEntity.ok(success(stockService.findStocks()));
    }

    @GetMapping("/latest")
    public ResponseEntity<BaseResponse<List<StockTickResponse>>> findLatestBatch(@RequestParam String symbols) {
        checkAccess("read");
        return ResponseEntity.ok(success(stockService.findLatestBatch(symbols)));
    }

    @GetMapping("/{symbol}")
    public ResponseEntity<BaseResponse<StockResponse>> findStock(@PathVariable String symbol) {
        checkAccess("read");
        return ResponseEntity.ok(success(stockService.findStock(symbol)));
    }

    @GetMapping("/{symbol}/daily")
    public ResponseEntity<BaseResponse<List<StockResponse>>> findDaily(@PathVariable String symbol) {
        checkAccess("read");
        return ResponseEntity.ok(success(stockService.findDaily(symbol)));
    }

    @GetMapping("/{symbol}/latest")
    public ResponseEntity<BaseResponse<StockTickResponse>> findLatest(@PathVariable String symbol) {
        checkAccess("read");
        return ResponseEntity.ok(success(stockService.findLatest(symbol)));
    }

    @GetMapping("/{symbol}/ohlc")
    public ResponseEntity<BaseResponse<List<StockOhlcResponse>>> findOhlc(@PathVariable String symbol) {
        checkAccess("read");
        return ResponseEntity.ok(success(stockService.findOhlc(symbol)));
    }

    private void checkAccess(String action) {
        SecurityUtil.checkAnyRole(List.of("ADMIN", "USER"));
        SecurityUtil.checkPermission("stock", action);
    }

    private <T> BaseResponse<T> success(T data) {
        return BaseResponse.<T>builder()
                .code(HttpStatus.OK)
                .message(SuccessMessageKey.SUCCESS)
                .data(data)
                .build();
    }
}
