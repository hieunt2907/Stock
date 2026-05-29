package com.hieunt.stock.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.response.StockResponse;
import com.hieunt.stock.service.StockService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockController {

    private final StockService stockService;

    @GetMapping
    public ResponseEntity<BaseResponse<List<StockResponse>>> findStocks() {
        return ResponseEntity.ok(BaseResponse.<List<StockResponse>>builder()
                .code(HttpStatus.OK)
                .message(SuccessMessageKey.SUCCESS)
                .data(stockService.findStocks())
                .build());
    }
}
