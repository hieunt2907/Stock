package com.hieunt.stock.service.impl;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import com.hieunt.stock.model.response.StockOhlcResponse;
import com.hieunt.stock.model.response.StockResponse;
import com.hieunt.stock.model.response.StockTickResponse;
import com.hieunt.stock.service.StockService;

@Service
public class StockServiceImpl implements StockService {

    private final JdbcTemplate clickHouseJdbcTemplate;

    public StockServiceImpl(
            @Value("${app.clickhouse.host}") String host,
            @Value("${app.clickhouse.http-port}") Integer httpPort,
            @Value("${app.clickhouse.database}") String database,
            @Value("${app.clickhouse.username}") String username,
            @Value("${app.clickhouse.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(String.format("jdbc:clickhouse://%s:%d/%s", host, httpPort, database));
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        this.clickHouseJdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public List<StockResponse> findStocks() {
        return clickHouseJdbcTemplate.query(latestDailySql("") + " ORDER BY f.symbol", (rs, rowNum) -> mapStock(rs));
    }

    @Override
    public StockResponse findStock(String symbol) {
        String sql = latestDailySql("WHERE f.symbol = ?") + " ORDER BY f.trading_date DESC LIMIT 1";
        return clickHouseJdbcTemplate.queryForObject(sql, (rs, rowNum) -> mapStock(rs), normalizeSymbol(symbol));
    }

    @Override
    public List<StockResponse> findDaily(String symbol) {
        String sql = ""
                + "SELECT "
                + "  f.symbol AS symbol, "
                + "  ifNull(d.company_name, '') AS company_name, "
                + "  ifNull(d.exchange, '') AS exchange, "
                + "  ifNull(d.industry, '') AS industry, "
                + "  ifNull(d.sector, '') AS sector, "
                + "  f.trading_date AS trading_date, "
                + "  f.open AS open, "
                + "  f.high AS high, "
                + "  f.low AS low, "
                + "  f.close AS close, "
                + "  f.volume AS volume, "
                + "  f.value AS value, "
                + "  f.daily_return AS daily_return, "
                + "  f.volatility AS volatility, "
                + "  f.source AS source "
                + "FROM (SELECT * FROM fact_stock_daily FINAL) AS f "
                + "LEFT JOIN (SELECT * FROM dim_company FINAL) AS d ON f.symbol = d.symbol "
                + "WHERE f.symbol = ? "
                + "ORDER BY f.trading_date DESC";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapStock(rs), normalizeSymbol(symbol));
    }

    @Override
    public StockTickResponse findLatest(String symbol) {
        String sql = ""
                + "SELECT "
                + "  f.event_id AS event_id, "
                + "  f.symbol AS symbol, "
                + "  f.event_time AS event_time, "
                + "  f.price AS price, "
                + "  f.volume AS volume, "
                + "  f.source AS source "
                + "FROM (SELECT * FROM fact_stock_tick FINAL) AS f "
                + "WHERE f.symbol = ? "
                + "ORDER BY f.event_time DESC "
                + "LIMIT 1";

        return clickHouseJdbcTemplate.queryForObject(sql, (rs, rowNum) -> mapTick(rs), normalizeSymbol(symbol));
    }

    @Override
    public List<StockTickResponse> findLatestBatch(String symbols) {
        List<String> normalizedSymbols = parseSymbols(symbols);
        String placeholders = normalizedSymbols.stream()
                .map(symbol -> "?")
                .collect(Collectors.joining(", "));
        String sql = ""
                + "SELECT "
                + "  f.event_id AS event_id, "
                + "  f.symbol AS symbol, "
                + "  f.event_time AS event_time, "
                + "  f.price AS price, "
                + "  f.volume AS volume, "
                + "  f.source AS source "
                + "FROM (SELECT * FROM fact_stock_tick FINAL) AS f "
                + "INNER JOIN ( "
                + "  SELECT symbol, max(event_time) AS event_time "
                + "  FROM fact_stock_tick FINAL "
                + "  WHERE symbol IN (" + placeholders + ") "
                + "  GROUP BY symbol "
                + ") AS latest ON f.symbol = latest.symbol AND f.event_time = latest.event_time "
                + "ORDER BY f.symbol";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapTick(rs), normalizedSymbols.toArray());
    }

    @Override
    public List<StockOhlcResponse> findOhlc(String symbol) {
        String sql = ""
                + "SELECT "
                + "  f.symbol AS symbol, "
                + "  f.window_start AS window_start, "
                + "  f.window_end AS window_end, "
                + "  f.open AS open, "
                + "  f.high AS high, "
                + "  f.low AS low, "
                + "  f.close AS close, "
                + "  f.volume AS volume, "
                + "  f.value AS value, "
                + "  f.source AS source "
                + "FROM (SELECT * FROM fact_stock_ohlc_1m FINAL) AS f "
                + "WHERE f.symbol = ? "
                + "ORDER BY f.window_start DESC";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapOhlc(rs), normalizeSymbol(symbol));
    }

    private String latestDailySql(String whereClause) {
        return ""
                + "SELECT "
                + "  f.symbol AS symbol, "
                + "  ifNull(d.company_name, '') AS company_name, "
                + "  ifNull(d.exchange, '') AS exchange, "
                + "  ifNull(d.industry, '') AS industry, "
                + "  ifNull(d.sector, '') AS sector, "
                + "  f.trading_date AS trading_date, "
                + "  f.open AS open, "
                + "  f.high AS high, "
                + "  f.low AS low, "
                + "  f.close AS close, "
                + "  f.volume AS volume, "
                + "  f.value AS value, "
                + "  f.daily_return AS daily_return, "
                + "  f.volatility AS volatility, "
                + "  f.source AS source "
                + "FROM (SELECT * FROM fact_stock_daily FINAL) AS f "
                + "INNER JOIN ( "
                + "  SELECT symbol, max(trading_date) AS trading_date "
                + "  FROM fact_stock_daily FINAL "
                + "  GROUP BY symbol "
                + ") AS latest ON f.symbol = latest.symbol AND f.trading_date = latest.trading_date "
                + "LEFT JOIN (SELECT * FROM dim_company FINAL) AS d ON f.symbol = d.symbol "
                + whereClause + " ";
    }

    private StockResponse mapStock(ResultSet rs) throws SQLException {
        return StockResponse.builder()
                .symbol(rs.getString("symbol"))
                .companyName(rs.getString("company_name"))
                .exchange(rs.getString("exchange"))
                .industry(rs.getString("industry"))
                .sector(rs.getString("sector"))
                .tradingDate(toLocalDate(rs.getDate("trading_date")))
                .open(rs.getBigDecimal("open"))
                .high(rs.getBigDecimal("high"))
                .low(rs.getBigDecimal("low"))
                .close(rs.getBigDecimal("close"))
                .volume(rs.getLong("volume"))
                .value(rs.getBigDecimal("value"))
                .dailyReturn(rs.getBigDecimal("daily_return"))
                .volatility(rs.getBigDecimal("volatility"))
                .source(rs.getString("source"))
                .build();
    }

    private LocalDate toLocalDate(Date date) {
        return date == null ? null : date.toLocalDate();
    }

    private StockTickResponse mapTick(ResultSet rs) throws SQLException {
        return StockTickResponse.builder()
                .eventId(rs.getString("event_id"))
                .symbol(rs.getString("symbol"))
                .eventTime(toLocalDateTime(rs.getTimestamp("event_time")))
                .price(rs.getBigDecimal("price"))
                .volume(rs.getLong("volume"))
                .source(rs.getString("source"))
                .build();
    }

    private StockOhlcResponse mapOhlc(ResultSet rs) throws SQLException {
        return StockOhlcResponse.builder()
                .symbol(rs.getString("symbol"))
                .windowStart(toLocalDateTime(rs.getTimestamp("window_start")))
                .windowEnd(toLocalDateTime(rs.getTimestamp("window_end")))
                .open(rs.getBigDecimal("open"))
                .high(rs.getBigDecimal("high"))
                .low(rs.getBigDecimal("low"))
                .close(rs.getBigDecimal("close"))
                .volume(rs.getLong("volume"))
                .value(rs.getBigDecimal("value"))
                .source(rs.getString("source"))
                .build();
    }

    private LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private String normalizeSymbol(String symbol) {
        return symbol.trim().toUpperCase();
    }

    private List<String> parseSymbols(String symbols) {
        return Arrays.stream(symbols.split(","))
                .map(this::normalizeSymbol)
                .filter(symbol -> !symbol.isBlank())
                .distinct()
                .collect(Collectors.toList());
    }
}
