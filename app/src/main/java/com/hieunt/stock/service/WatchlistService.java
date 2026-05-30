package com.hieunt.stock.service;

import java.util.List;

import com.hieunt.stock.model.response.WatchlistResponse;

public interface WatchlistService {
    List<WatchlistResponse> findCurrentUserWatchlist();

    List<WatchlistResponse> addSymbol(String symbol);

    List<WatchlistResponse> removeSymbol(String symbol);
}
