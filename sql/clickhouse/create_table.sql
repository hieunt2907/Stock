-- ============================================================
--  ClickHouse Star Schema — Stock Data Warehouse
--  Database: stock
-- ============================================================

CREATE DATABASE IF NOT EXISTS stock;

-- ============================================================
--  DIM TABLES
-- ============================================================

-- dim_ticker: thông tin cơ bản mỗi mã cổ phiếu
CREATE TABLE IF NOT EXISTS stock.dim_ticker (
    ticker              String,
    group        String,          -- "group" từ ticker_list
    -- SCD Type 1 (latest snapshot)
) ENGINE = ReplacingMergeTree()
ORDER BY (ticker);

-- dim_company: thông tin công ty (SCD Type 2 — giữ version)
CREATE TABLE IF NOT EXISTS stock.dim_company (
    ticker              String,          -- audit version từ Postgres
    symbol              String,
    business_model      String,
    founded_date        String,
    charter_capital     Decimal(30, 2),
    number_of_employees Int64,
    listing_date        String,
    par_value           Int64,
    exchange            String,
    listing_price       Int64,
    listed_volume       Int64,
    ceo_name            String,,
    ceo_position        String,
    inspector_name      String,
    inspector_position  String,
    establishment_license String,
    business_code       String,
    tax_id              String,
    auditor             String,
    company_type        String,
    address             String,
    phone               String,
    fax                 String,
    email               String,
    website             String,
    branches            String,
    history             String,
    free_float_percentage Decimal(10, 4),
    free_float          Decimal(30, 2),
    outstanding_shares  Int64,
    as_of_date          DateTime,
) ENGINE = ReplacingMergeTree()
ORDER BY (ticker);

-- dim_date: bảng thời gian (calendar table)
CREATE TABLE IF NOT EXISTS stock.dim_date (
    date_id     Date,
    year        Int16,
    quarter     Int8,
    month       Int8,
    week        Int8,
    day_of_month Int8,
    day_of_week Int8,       -- 1 = Mon, 7 = Sun
    is_weekend  UInt8,
) ENGINE = ReplacingMergeTree()
ORDER BY (date_id);

-- ============================================================
--  FACT TABLES
-- ============================================================

-- fact_daily_price: giá cổ phiếu hàng ngày
CREATE TABLE IF NOT EXISTS stock.fact_daily_price (
    ticker      String,
    time        DateTime,
    date_id     Date,               -- FK → dim_date
    open        Decimal(20, 2),
    high        Decimal(20, 2),
    low         Decimal(20, 2),
    close       Decimal(20, 2),
    volume      Int64,
) ENGINE = ReplacingMergeTree()
PARTITION BY ticker
ORDER BY (ticker, time);

-- fact_benchmark_index: chỉ số thị trường (VN-Index, HNX, ...)
CREATE TABLE IF NOT EXISTS stock.fact_benchmark_index (
    ticker      String,
    time        DateTime,
    date_id     Date,               -- FK → dim_date
    open        Decimal(20, 2),
    high        Decimal(20, 2),
    low         Decimal(20, 2),
    close       Decimal(20, 2),
    volume      Int64,
) ENGINE = ReplacingMergeTree()
PARTITION BY ticker
ORDER BY (ticker, time);

-- ============================================================
--  ANALYTICAL / DERIVED TABLES
-- ============================================================

-- fact_financial_ratio: chỉ số tài chính theo quý/năm từ Finance().financial_ratio()
--   Source: MinIO financial_ratio/<partition>/<ticker>.json
--   Cập nhật: @weekly (sau mỗi mùa báo cáo tài chính)
CREATE TABLE IF NOT EXISTS stock.fact_financial_ratio (
    ticker              String,
    year                Int32,                      -- Năm báo cáo (2023, 2024, ...)
    quarter             String,                     -- "Q1" | "Q2" | "Q3" | "Q4" | "Yearly"

    -- Định giá
    pe                  Nullable(Decimal(20, 4)),   -- Price-to-Earnings
    pb                  Nullable(Decimal(20, 4)),   -- Price-to-Book
    ps                  Nullable(Decimal(20, 4)),   -- Price-to-Sales
    ev_ebitda           Nullable(Decimal(20, 4)),   -- EV/EBITDA

    -- Khả năng sinh lời
    roe                 Nullable(Decimal(20, 4)),   -- Return on Equity (%)
    roa                 Nullable(Decimal(20, 4)),   -- Return on Assets (%)
    eps                 Nullable(Decimal(20, 4)),   -- Earnings Per Share
    net_profit_margin   Nullable(Decimal(20, 4)),   -- Biên lợi nhuận ròng (%)
    gross_profit_margin Nullable(Decimal(20, 4)),   -- Biên lợi nhuận gộp (%)

    -- Hiệu quả hoạt động
    asset_turnover      Nullable(Decimal(20, 4)),   -- Vòng quay tài sản

    -- Thanh khoản & đòn bẩy
    current_ratio       Nullable(Decimal(20, 4)),   -- Tỷ số thanh toán hiện hành
    debt_to_equity      Nullable(Decimal(20, 4)),   -- Nợ / Vốn chủ sở hữu
) ENGINE = ReplacingMergeTree()
ORDER BY (ticker, year, quarter)
COMMENT 'Chỉ số tài chính theo quý — nguồn: Finance().financial_ratio() qua VCI';

-- ============================================================
--  REALTIME TABLES
-- ============================================================

-- fact_intraday_tick: dữ liệu khớp lệnh theo giây — nguồn Kafka intraday_tick
CREATE TABLE IF NOT EXISTS stock.fact_intraday_tick (
    ticker      String,
    time        DateTime,
    price       Decimal(20, 2),
    volume      Int64,
    match_type  String,          -- ATO | ATC | LO
    buy_vol     Int64,
    sell_vol    Int64,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
PARTITION BY (ticker, toYYYYMMDD(time))
ORDER BY (ticker, time)
COMMENT 'Khớp lệnh realtime (30s poll) — Spark Structured Streaming từ Kafka topic intraday_tick';

-- technical_indicators_historical: chỉ số dài hạn tính từ daily_price_backfill
--   RSI(14) — đánh giá xu hướng trung/dài hạn
--   SMA(200) — đường trung bình 200 ngày (benchmark trend)
--   Chạy 1 lần (backfill), rerun khi đổi công thức
-- CREATE TABLE IF NOT EXISTS stock.technical_indicators_historical (
--     ticker        String,
--     time          DateTime,
--     date_id       Date,
--     close         Decimal(20, 2),
--     rsi_14        Nullable(Float64),
--     sma_200       Nullable(Float64),
--     calculated_at DateTime DEFAULT now()
-- ) ENGINE = ReplacingMergeTree(calculated_at)
-- PARTITION BY toYYYYMM(time)
-- ORDER BY (ticker, time);

-- -- technical_indicators_daily: chỉ số ngắn hạn tính từ daily_price (incremental hàng ngày)
-- --   RSI(3)  — phát hiện overbought/oversold ngắn hạn
-- --   SMA(10) — đường trung bình 10 ngày (momentum ngắn hạn)
-- --   Append mỗi ngày sau khi daily_price được nạp vào Postgres
-- CREATE TABLE IF NOT EXISTS stock.technical_indicators_daily (
--     ticker        String,
--     time          DateTime,
--     date_id       Date,
--     close         Decimal(20, 2),
--     rsi_3         Float64,
--     sma_10        Float64,
--     calculated_at DateTime DEFAULT now()
-- ) ENGINE = ReplacingMergeTree(calculated_at)
-- PARTITION BY toYYYYMM(time)
-- ORDER BY (ticker, time);
