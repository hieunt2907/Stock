package com.hieunt.stock.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import com.hieunt.stock.model.response.CompanyResponse;
import com.hieunt.stock.service.CompanyService;

@Service
public class CompanyServiceImpl implements CompanyService {

    private final JdbcTemplate clickHouseJdbcTemplate;

    public CompanyServiceImpl(
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
    public List<CompanyResponse> findCompanies() {
        String sql = companySelectSql()
                + "FROM (SELECT * FROM dim_company FINAL) AS d "
                + "ORDER BY d.symbol";

        return clickHouseJdbcTemplate.query(sql, (rs, rowNum) -> mapCompany(rs));
    }

    @Override
    public CompanyResponse findCompany(String symbol) {
        String sql = companySelectSql()
                + "FROM (SELECT * FROM dim_company FINAL) AS d "
                + "WHERE d.symbol = ? "
                + "LIMIT 1";

        return clickHouseJdbcTemplate.queryForObject(sql, (rs, rowNum) -> mapCompany(rs), normalizeSymbol(symbol));
    }

    private String companySelectSql() {
        return ""
                + "SELECT "
                + "  d.symbol AS symbol, "
                + "  d.company_name AS company_name, "
                + "  d.exchange AS exchange, "
                + "  d.industry AS industry, "
                + "  d.sector AS sector, "
                + "  d.country AS country, "
                + "  d.currency AS currency, "
                + "  d.website AS website, "
                + "  d.description AS description, "
                + "  d.market_cap AS market_cap, "
                + "  d.shares_outstanding AS shares_outstanding, "
                + "  d.source AS source, "
                + "  d.updated_at AS updated_at ";
    }

    private CompanyResponse mapCompany(ResultSet rs) throws SQLException {
        return CompanyResponse.builder()
                .symbol(rs.getString("symbol"))
                .companyName(rs.getString("company_name"))
                .exchange(rs.getString("exchange"))
                .industry(rs.getString("industry"))
                .sector(rs.getString("sector"))
                .country(rs.getString("country"))
                .currency(rs.getString("currency"))
                .website(rs.getString("website"))
                .description(rs.getString("description"))
                .marketCap(rs.getBigDecimal("market_cap"))
                .sharesOutstanding(rs.getBigDecimal("shares_outstanding"))
                .source(rs.getString("source"))
                .updatedAt(toLocalDateTime(rs.getTimestamp("updated_at")))
                .build();
    }

    private LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private String normalizeSymbol(String symbol) {
        return symbol.trim().toUpperCase();
    }
}
