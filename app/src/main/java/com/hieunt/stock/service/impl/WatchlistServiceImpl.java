package com.hieunt.stock.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.hieunt.stock.model.response.WatchlistResponse;
import com.hieunt.stock.repository.WatchlistRepository;
import com.hieunt.stock.repository.entity.WatchlistEntity;
import com.hieunt.stock.service.WatchlistService;
import com.hieunt.stock.util.SecurityUtil;

@Service
public class WatchlistServiceImpl implements WatchlistService {

    private final WatchlistRepository watchlistRepository;
    private final JdbcTemplate clickHouseJdbcTemplate;

    public WatchlistServiceImpl(
            WatchlistRepository watchlistRepository,
            @Value("${app.clickhouse.host}") String host,
            @Value("${app.clickhouse.http-port}") Integer httpPort,
            @Value("${app.clickhouse.database}") String database,
            @Value("${app.clickhouse.username}") String username,
            @Value("${app.clickhouse.password}") String password) {
        this.watchlistRepository = watchlistRepository;
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(String.format("jdbc:clickhouse://%s:%d/%s", host, httpPort, database));
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        this.clickHouseJdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    @Transactional(readOnly = true)
    public List<WatchlistResponse> findCurrentUserWatchlist() {
        List<String> symbols = findSymbols(getCurrentCreatedBy());
        if (symbols.isEmpty()) {
            return List.of();
        }
        return findWatchlistMarketData(symbols);
    }

    @Override
    @Transactional
    public List<WatchlistResponse> addSymbol(String symbol) {
        String createdBy = getCurrentCreatedBy();
        String normalizedSymbol = normalizeSymbol(symbol);
        watchlistRepository.findByCreatedByAndSymbol(createdBy, normalizedSymbol)
                .orElseGet(() -> watchlistRepository.save(new WatchlistEntity(normalizedSymbol)));
        return findCurrentUserWatchlist();
    }

    @Override
    @Transactional
    public List<WatchlistResponse> removeSymbol(String symbol) {
        String createdBy = getCurrentCreatedBy();
        watchlistRepository.findByCreatedByAndSymbol(createdBy, normalizeSymbol(symbol))
                .ifPresent(watchlistRepository::delete);
        return findCurrentUserWatchlist();
    }

    private List<String> findSymbols(String createdBy) {
        return watchlistRepository.findByCreatedByOrderBySymbolAsc(createdBy).stream()
                .map(WatchlistEntity::getSymbol)
                .collect(Collectors.toList());
    }

    private String getCurrentCreatedBy() {
        return SecurityUtil.getCurrentUsername()
                .orElseThrow(() -> new org.springframework.security.access.AccessDeniedException("Unauthenticated"));
    }

    private List<WatchlistResponse> findWatchlistMarketData(List<String> symbols) {
        String arrayLiteral = symbols.stream()
                .map(this::toClickHouseStringLiteral)
                .collect(Collectors.joining(", "));
        String sql = ""
                + "SELECT "
                + "  s.symbol AS symbol, "
                + "  ifNull(d.company_name, '') AS company_name, "
                + "  ifNull(d.exchange, '') AS exchange, "
                + "  ifNull(d.industry, '') AS industry, "
                + "  ifNull(d.sector, '') AS sector, "
                + "  t.event_time AS event_time, "
                + "  t.price AS price, "
                + "  t.volume AS volume, "
                + "  ifNull(t.source, '') AS source "
                + "FROM (SELECT arrayJoin([" + arrayLiteral + "]) AS symbol) AS s "
                + "LEFT JOIN (SELECT * FROM dim_company FINAL) AS d ON s.symbol = d.symbol "
                + "LEFT JOIN ( "
                + "  SELECT f.event_id, f.symbol, f.event_time, f.price, f.volume, f.source "
                + "  FROM (SELECT * FROM fact_stock_tick FINAL) AS f "
                + "  INNER JOIN ( "
                + "    SELECT symbol, max(event_time) AS event_time "
                + "    FROM fact_stock_tick FINAL "
                + "    WHERE symbol IN (" + arrayLiteral + ") "
                + "    GROUP BY symbol "
                + "  ) AS latest ON f.symbol = latest.symbol AND f.event_time = latest.event_time "
                + ") AS t ON s.symbol = t.symbol "
                + "ORDER BY s.symbol";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapWatchlist(rs));
    }

    private WatchlistResponse mapWatchlist(ResultSet rs) throws SQLException {
        return WatchlistResponse.builder()
                .symbol(rs.getString("symbol"))
                .companyName(rs.getString("company_name"))
                .exchange(rs.getString("exchange"))
                .industry(rs.getString("industry"))
                .sector(rs.getString("sector"))
                .eventTime(toLocalDateTime(rs.getTimestamp("event_time")))
                .price(rs.getBigDecimal("price"))
                .volume(rs.getObject("volume") == null ? null : rs.getLong("volume"))
                .source(rs.getString("source"))
                .build();
    }

    private LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private String normalizeSymbol(String symbol) {
        return symbol.trim().toUpperCase();
    }

    private String toClickHouseStringLiteral(String symbol) {
        String normalizedSymbol = normalizeSymbol(symbol);
        if (!normalizedSymbol.matches("[A-Z0-9._-]{1,20}")) {
            throw new IllegalArgumentException("Invalid symbol: " + symbol);
        }
        return "'" + normalizedSymbol + "'";
    }
}
