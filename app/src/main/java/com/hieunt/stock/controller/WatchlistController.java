package com.hieunt.stock.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.model.response.WatchlistResponse;
import com.hieunt.stock.service.WatchlistService;
import com.hieunt.stock.util.SecurityUtil;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/watchlists")
@RequiredArgsConstructor
public class WatchlistController {

    private final WatchlistService watchlistService;

    @GetMapping
    public ResponseEntity<BaseResponse<List<WatchlistResponse>>> findWatchlist() {
        checkAccess("read");
        return ResponseEntity.ok(success(HttpStatus.OK, watchlistService.findCurrentUserWatchlist()));
    }

    @PostMapping("/{symbol}")
    public ResponseEntity<BaseResponse<List<WatchlistResponse>>> addSymbol(@PathVariable String symbol) {
        checkAccess("create");
        return ResponseEntity.ok(success(HttpStatus.CREATED, watchlistService.addSymbol(symbol)));
    }

    @DeleteMapping("/{symbol}")
    public ResponseEntity<BaseResponse<List<WatchlistResponse>>> removeSymbol(@PathVariable String symbol) {
        checkAccess("delete");
        return ResponseEntity.ok(success(HttpStatus.OK, watchlistService.removeSymbol(symbol)));
    }

    private void checkAccess(String action) {
        SecurityUtil.checkAnyRole(List.of("ADMIN", "ANALYST", "USER"));
        SecurityUtil.checkPermission("watchlist", action);
    }

    private <T> BaseResponse<T> success(HttpStatus status, T data) {
        return BaseResponse.<T>builder()
                .code(status)
                .message(SuccessMessageKey.SUCCESS)
                .data(data)
                .build();
    }
}
