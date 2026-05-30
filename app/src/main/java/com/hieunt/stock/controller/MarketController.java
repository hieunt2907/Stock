package com.hieunt.stock.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.response.MarketSectorResponse;
import com.hieunt.stock.model.response.MarketSummaryResponse;
import com.hieunt.stock.model.response.MarketTopGainerResponse;
import com.hieunt.stock.model.response.MarketTopLiquidityResponse;
import com.hieunt.stock.service.MarketService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/market")
@RequiredArgsConstructor
public class MarketController {

    private final MarketService marketService;

    @GetMapping("/summary")
    public ResponseEntity<BaseResponse<MarketSummaryResponse>> getSummary() {
        return ResponseEntity.ok(success(marketService.getSummary()));
    }

    @GetMapping("/top-gainers")
    public ResponseEntity<BaseResponse<List<MarketTopGainerResponse>>> getTopGainers() {
        return ResponseEntity.ok(success(marketService.getTopGainers()));
    }

    @GetMapping("/top-liquidity")
    public ResponseEntity<BaseResponse<List<MarketTopLiquidityResponse>>> getTopLiquidity() {
        return ResponseEntity.ok(success(marketService.getTopLiquidity()));
    }

    @GetMapping("/sectors")
    public ResponseEntity<BaseResponse<List<MarketSectorResponse>>> getSectors() {
        return ResponseEntity.ok(success(marketService.getSectors()));
    }

    private <T> BaseResponse<T> success(T data) {
        return BaseResponse.<T>builder()
                .code(HttpStatus.OK)
                .message(SuccessMessageKey.SUCCESS)
                .data(data)
                .build();
    }
}
