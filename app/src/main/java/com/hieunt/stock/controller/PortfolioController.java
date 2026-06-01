package com.hieunt.stock.controller;

import java.util.List;

import javax.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.request.PortfolioRequest;
import com.hieunt.stock.model.request.PortfolioTransactionRequest;
import com.hieunt.stock.model.response.PortfolioHoldingResponse;
import com.hieunt.stock.model.response.PortfolioPerformanceResponse;
import com.hieunt.stock.model.response.PortfolioResponse;
import com.hieunt.stock.model.response.PortfolioTransactionResponse;
import com.hieunt.stock.service.PortfolioService;
import com.hieunt.stock.util.SecurityUtil;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/portfolios")
@RequiredArgsConstructor
public class PortfolioController {

    private final PortfolioService portfolioService;

    @GetMapping
    public ResponseEntity<BaseResponse<List<PortfolioResponse>>> findPortfolios() {
        checkAccess("read");
        return ResponseEntity.ok(success(HttpStatus.OK, portfolioService.findCurrentUserPortfolios()));
    }

    @PostMapping
    public ResponseEntity<BaseResponse<PortfolioResponse>> createPortfolio(
            @Valid @RequestBody PortfolioRequest request) {
        checkAccess("create");
        return ResponseEntity.ok(success(HttpStatus.CREATED, portfolioService.createPortfolio(request)));
    }

    @GetMapping("/{id}/holdings")
    public ResponseEntity<BaseResponse<List<PortfolioHoldingResponse>>> getHoldings(@PathVariable Long id)
            throws HIEUNTException {
        checkAccess("read");
        return ResponseEntity.ok(success(HttpStatus.OK, portfolioService.getHoldings(id)));
    }

    @PostMapping("/{id}/transactions")
    public ResponseEntity<BaseResponse<PortfolioTransactionResponse>> addTransaction(
            @PathVariable Long id,
            @Valid @RequestBody PortfolioTransactionRequest request) throws HIEUNTException {
        checkAccess("create");
        return ResponseEntity.ok(success(HttpStatus.CREATED, portfolioService.addTransaction(id, request)));
    }

    @GetMapping("/{id}/performance")
    public ResponseEntity<BaseResponse<PortfolioPerformanceResponse>> getPerformance(@PathVariable Long id)
            throws HIEUNTException {
        checkAccess("read");
        return ResponseEntity.ok(success(HttpStatus.OK, portfolioService.getPerformance(id)));
    }

    private void checkAccess(String action) {
        SecurityUtil.checkAnyRole(List.of("ADMIN", "ANALYST", "USER"));
        SecurityUtil.checkPermission("portfolio", action);
    }

    private <T> BaseResponse<T> success(HttpStatus status, T data) {
        return BaseResponse.<T>builder()
                .code(status)
                .message(SuccessMessageKey.SUCCESS)
                .data(data)
                .build();
    }
}
