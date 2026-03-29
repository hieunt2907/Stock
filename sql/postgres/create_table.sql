-- Table: ticker_list
CREATE TABLE IF NOT EXISTS stock.ticker_list (
    ticker VARCHAR(20) PRIMARY KEY,
    "group" VARCHAR(50)
);

-- Table: company_metadata
CREATE TABLE IF NOT EXISTS stock.company_metadata (
    symbol VARCHAR(20),
    business_model TEXT,
    founded_date VARCHAR(50),
    charter_capital NUMERIC,
    number_of_employees BIGINT,
    listing_date VARCHAR(50),
    par_value BIGINT,
    exchange VARCHAR(50),
    listing_price BIGINT,
    listed_volume BIGINT,
    ceo_name VARCHAR(255),
    ceo_position VARCHAR(255),
    inspector_name VARCHAR(255),
    inspector_position VARCHAR(255),
    establishment_license VARCHAR(255),
    business_code VARCHAR(255),
    tax_id VARCHAR(50),
    auditor VARCHAR(255),
    company_type VARCHAR(255),
    address TEXT,
    phone VARCHAR(255),
    fax VARCHAR(255),
    email VARCHAR(255),
    website VARCHAR(255),
    branches TEXT,
    history TEXT,
    free_float_percentage NUMERIC,
    free_float NUMERIC,
    outstanding_shares BIGINT,
    as_of_date TIMESTAMP,
    ticker VARCHAR(20),
    PRIMARY KEY (ticker)
);

-- Table: benchmark_index
CREATE TABLE IF NOT EXISTS stock.benchmark_index (
    time TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    ticker VARCHAR(20),
    PRIMARY KEY (ticker, time)
);

-- Table: daily_price_backfill
CREATE TABLE IF NOT EXISTS stock.daily_price_backfill (
    time TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    ticker VARCHAR(20),
    PRIMARY KEY (ticker, time)
);

-- Table: daily_price
CREATE TABLE IF NOT EXISTS stock.daily_price (
    time TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    ticker VARCHAR(20),
    PRIMARY KEY (ticker, time)
);
