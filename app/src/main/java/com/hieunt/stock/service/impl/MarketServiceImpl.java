package com.hieunt.stock.service.impl;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import com.hieunt.stock.model.response.MarketSectorResponse;
import com.hieunt.stock.model.response.MarketSummaryResponse;
import com.hieunt.stock.model.response.MarketTopGainerResponse;
import com.hieunt.stock.model.response.MarketTopLiquidityResponse;
import com.hieunt.stock.service.MarketService;

@Service
public class MarketServiceImpl implements MarketService {

    private final JdbcTemplate clickHouseJdbcTemplate;

    public MarketServiceImpl(
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
    public MarketSummaryResponse getSummary() {
        String sql = ""
                + "SELECT "
                + "  m.trading_date AS trading_date, "
                + "  m.total_symbols AS total_symbols, "
                + "  m.total_volume AS total_volume, "
                + "  m.total_value AS total_value, "
                + "  m.avg_return AS avg_return, "
                + "  m.advancers AS advancers, "
                + "  m.decliners AS decliners, "
                + "  m.unchanged AS unchanged, "
                + "  m.ingestion_time AS ingestion_time "
                + "FROM (SELECT * FROM mart_market_summary FINAL) AS m "
                + "ORDER BY m.trading_date DESC "
                + "LIMIT 1";

        return clickHouseJdbcTemplate.queryForObject(sql, (rs, rowNum) -> mapSummary(rs));
    }

    @Override
    public List<MarketTopGainerResponse> getTopGainers() {
        String sql = ""
                + "SELECT "
                + "  m.trading_date AS trading_date, "
                + "  m.symbol AS symbol, "
                + "  m.company_name AS company_name, "
                + "  m.sector AS sector, "
                + "  m.close AS close, "
                + "  m.daily_return AS daily_return, "
                + "  m.rank AS rank, "
                + "  m.ingestion_time AS ingestion_time "
                + "FROM (SELECT * FROM mart_top_gainers FINAL) AS m "
                + "INNER JOIN (SELECT max(trading_date) AS trading_date FROM mart_top_gainers FINAL) AS latest "
                + "ON m.trading_date = latest.trading_date "
                + "ORDER BY m.rank ASC";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapTopGainer(rs));
    }

    @Override
    public List<MarketTopLiquidityResponse> getTopLiquidity() {
        String sql = ""
                + "SELECT "
                + "  m.trading_date AS trading_date, "
                + "  m.symbol AS symbol, "
                + "  m.company_name AS company_name, "
                + "  m.sector AS sector, "
                + "  m.volume AS volume, "
                + "  m.value AS value, "
                + "  m.rank AS rank, "
                + "  m.ingestion_time AS ingestion_time "
                + "FROM (SELECT * FROM mart_top_liquidity FINAL) AS m "
                + "INNER JOIN (SELECT max(trading_date) AS trading_date FROM mart_top_liquidity FINAL) AS latest "
                + "ON m.trading_date = latest.trading_date "
                + "ORDER BY m.rank ASC";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapTopLiquidity(rs));
    }

    @Override
    public List<MarketSectorResponse> getSectors() {
        String sql = ""
                + "SELECT "
                + "  m.trading_date AS trading_date, "
                + "  m.sector AS sector, "
                + "  m.total_symbols AS total_symbols, "
                + "  m.total_volume AS total_volume, "
                + "  m.total_value AS total_value, "
                + "  m.avg_return AS avg_return, "
                + "  m.best_symbol AS best_symbol, "
                + "  m.worst_symbol AS worst_symbol, "
                + "  m.ingestion_time AS ingestion_time "
                + "FROM (SELECT * FROM mart_sector_performance FINAL) AS m "
                + "INNER JOIN (SELECT max(trading_date) AS trading_date FROM mart_sector_performance FINAL) AS latest "
                + "ON m.trading_date = latest.trading_date "
                + "ORDER BY m.avg_return DESC";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapSector(rs));
    }

    private MarketSummaryResponse mapSummary(ResultSet rs) throws SQLException {
        return MarketSummaryResponse.builder()
                .tradingDate(toLocalDate(rs.getDate("trading_date")))
                .totalSymbols(rs.getLong("total_symbols"))
                .totalVolume(rs.getLong("total_volume"))
                .totalValue(rs.getBigDecimal("total_value"))
                .avgReturn(rs.getBigDecimal("avg_return"))
                .advancers(rs.getLong("advancers"))
                .decliners(rs.getLong("decliners"))
                .unchanged(rs.getLong("unchanged"))
                .ingestionTime(toLocalDateTime(rs.getTimestamp("ingestion_time")))
                .build();
    }

    private MarketTopGainerResponse mapTopGainer(ResultSet rs) throws SQLException {
        return MarketTopGainerResponse.builder()
                .tradingDate(toLocalDate(rs.getDate("trading_date")))
                .symbol(rs.getString("symbol"))
                .companyName(rs.getString("company_name"))
                .sector(rs.getString("sector"))
                .close(rs.getBigDecimal("close"))
                .dailyReturn(rs.getBigDecimal("daily_return"))
                .rank(rs.getLong("rank"))
                .ingestionTime(toLocalDateTime(rs.getTimestamp("ingestion_time")))
                .build();
    }

    private MarketTopLiquidityResponse mapTopLiquidity(ResultSet rs) throws SQLException {
        return MarketTopLiquidityResponse.builder()
                .tradingDate(toLocalDate(rs.getDate("trading_date")))
                .symbol(rs.getString("symbol"))
                .companyName(rs.getString("company_name"))
                .sector(rs.getString("sector"))
                .volume(rs.getLong("volume"))
                .value(rs.getBigDecimal("value"))
                .rank(rs.getLong("rank"))
                .ingestionTime(toLocalDateTime(rs.getTimestamp("ingestion_time")))
                .build();
    }

    private MarketSectorResponse mapSector(ResultSet rs) throws SQLException {
        return MarketSectorResponse.builder()
                .tradingDate(toLocalDate(rs.getDate("trading_date")))
                .sector(rs.getString("sector"))
                .totalSymbols(rs.getLong("total_symbols"))
                .totalVolume(rs.getLong("total_volume"))
                .totalValue(rs.getBigDecimal("total_value"))
                .avgReturn(rs.getBigDecimal("avg_return"))
                .bestSymbol(rs.getString("best_symbol"))
                .worstSymbol(rs.getString("worst_symbol"))
                .ingestionTime(toLocalDateTime(rs.getTimestamp("ingestion_time")))
                .build();
    }

    private LocalDate toLocalDate(Date date) {
        return date == null ? null : date.toLocalDate();
    }

    private LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }
}
