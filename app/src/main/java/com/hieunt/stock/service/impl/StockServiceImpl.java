package com.hieunt.stock.service.impl;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import com.hieunt.stock.model.response.StockResponse;
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
        String sql = ""
                + "SELECT "
                + "  f.symbol, "
                + "  ifNull(d.company_name, '') AS company_name, "
                + "  ifNull(d.exchange, '') AS exchange, "
                + "  ifNull(d.industry, '') AS industry, "
                + "  ifNull(d.sector, '') AS sector, "
                + "  f.trading_date, "
                + "  f.open, f.high, f.low, f.close, "
                + "  f.volume, f.value, f.daily_return, f.volatility, f.source "
                + "FROM fact_stock_daily FINAL AS f "
                + "INNER JOIN ( "
                + "  SELECT symbol, max(trading_date) AS trading_date "
                + "  FROM fact_stock_daily FINAL "
                + "  GROUP BY symbol "
                + ") AS latest ON f.symbol = latest.symbol AND f.trading_date = latest.trading_date "
                + "LEFT JOIN dim_company FINAL AS d ON f.symbol = d.symbol "
                + "ORDER BY f.symbol";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapStock(rs));
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
}
