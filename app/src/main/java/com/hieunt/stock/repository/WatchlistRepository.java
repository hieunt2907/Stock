package com.hieunt.stock.repository;

import java.util.List;
import java.util.Optional;

import com.hieunt.stock.repository.entity.WatchlistEntity;

public interface WatchlistRepository extends BaseRepository<WatchlistEntity> {
    List<WatchlistEntity> findByCreatedByOrderBySymbolAsc(String createdBy);

    Optional<WatchlistEntity> findByCreatedByAndSymbol(String createdBy, String symbol);
}
