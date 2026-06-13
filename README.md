# Real-time Stock Intelligence Platform

A full-stack financial data platform that combines real-time market data streaming, batch ETL pipelines, and a user-facing web application for portfolio management and stock analytics.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Service URLs](#service-urls)
- [API Documentation](#api-documentation)
- [Environment Variables](#environment-variables)
- [Database Schema](#database-schema)

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────────┐
                        │              Data Sources                       │
                        │  Finnhub WebSocket  │  yfinance  │ Alpha Vantage│
                        └─────────┬───────────┴─────┬──────┴──────────────┘
                                  │                 │
                    ┌─────────────▼──────┐    ┌────▼──────────────────┐
                    │   Kafka (KRaft)    │    │   MinIO (Data Lake)   │
                    │   stock_tick_raw   │    │   stock-raw/          │
                    └─────────┬──────────┘    │   stock-canonical/    │
                              │               └────────────┬──────────┘
                    ┌─────────▼──────────┐                │
                    │  PySpark Streaming │    ┌───────────▼──────────┐
                    │  (OHLC Aggregation)│    │  PySpark Batch (ETL) │
                    └─────────┬──────────┘    │  via Apache Airflow  │
                              │               └───────────┬──────────┘
                              └──────────┬────────────────┘
                                         │
                              ┌──────────▼──────────┐
                              │      ClickHouse      │
                              │  fact_stock_tick     │
                              │  fact_stock_ohlc_1m  │
                              │  fact_stock_daily    │
                              │  dim_company         │
                              └──────────┬──────────┘
                                         │
                              ┌──────────▼──────────┐
                              │  Spring Boot API     │
                              │  (Port 8081)         │
                              │  + PostgreSQL        │
                              │  + Redis Cache       │
                              └──────────┬──────────┘
                                         │
                              ┌──────────▼──────────┐
                              │   Angular Frontend   │
                              │  Real-time Polling   │
                              └─────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Frontend** | Angular 21, TypeScript 5.9, Lightweight Charts, Nginx |
| **Backend API** | Spring Boot 2.7, Java 21, Maven, JWT, MapStruct |
| **Stream Processing** | Apache Kafka 7.7 (KRaft), PySpark 4.1 Structured Streaming |
| **Batch Orchestration** | Apache Airflow 2.9 |
| **Data Warehouse** | ClickHouse 24.8 (ReplacingMergeTree) |
| **Transactional DB** | PostgreSQL 15 |
| **Object Storage** | MinIO (S3-compatible) |
| **Cache** | Redis 7, Caffeine (in-process) |
| **Data Sources** | Finnhub WebSocket, yfinance, Alpha Vantage |
| **Containerization** | Docker, Docker Compose |

---

## Features

### Real-time Data Pipeline
- Live stock tick ingestion via Finnhub WebSocket → Kafka → Spark Structured Streaming
- 1-minute OHLC candle aggregation from raw tick data stored directly in ClickHouse
- Continuous background extraction and transformation services

### Batch Data Pipeline
- Daily OHLCV price history via yfinance with Airflow scheduling
- Company profile enrichment (sector, industry, market cap, website)
- Canonical data storage in MinIO before loading to ClickHouse
- Data mart aggregations for analytics queries

### Backend API (Spring Boot)
- JWT authentication with access/refresh token rotation
- Email OTP verification on registration
- Role-based access control (RBAC) with fine-grained permissions
- Portfolio management: create portfolios, record buy/sell transactions
- Watchlist: save and track favourite tickers
- Rate limiting (Bucket4j) and multi-layer caching (Redis + Caffeine)
- Swagger/OpenAPI documentation

### Frontend (Angular)
- Real-time price display with 60-second polling
- Stock list with sector/exchange filtering and multi-column sorting
- Candlestick and price charts (Lightweight Charts)
- Portfolio dashboard with holdings overview
- Watchlist management
- OTP-based registration flow

---

## Project Structure

```
Real-time-Stock-Intelligence-Platform/
├── app/                            # Spring Boot REST API
│   └── src/main/java/com/hieunt/stock/
│       ├── controller/             # REST endpoints (Auth, Stock, Company, Portfolio, Watchlist, RBAC)
│       ├── service/                # Business logic
│       ├── repository/             # JPA repositories (PostgreSQL)
│       │   └── entity/             # JPA entities
│       ├── model/                  # Request/Response DTOs
│       ├── config/                 # Security, Swagger, JPA audit, CORS
│       └── aspect/                 # AOP logging
│
├── frontend/                       # Angular 21 SPA
│   └── src/app/
│       ├── features/               # Route-level components (auth, dashboard, stocks, portfolio, watchlist)
│       ├── core/
│       │   ├── services/           # HTTP services (auth, stock, company, portfolio, watchlist)
│       │   ├── models/             # TypeScript interfaces
│       │   └── interceptors/       # JWT token interceptor
│       └── shared/                 # Sidebar, Topbar components
│
├── domain/stock/                   # Python data pipeline
│   ├── extract/                    # Raw data extraction scripts
│   │   ├── extract_finhub_streaming.py   # Finnhub WebSocket → Kafka
│   │   ├── extract_yfinance_daily.py     # yfinance daily prices → MinIO
│   │   └── extract_yfinance_company.py   # Company profiles → MinIO
│   ├── tranform/                   # Spark transformation jobs
│   │   ├── tranform_finnhub_streaming.py # Kafka → OHLC → ClickHouse (streaming)
│   │   ├── tranform_yfinace_daily.py     # MinIO raw → canonical (batch)
│   │   └── tranform_yfinance_company.py  # Company JSON → canonical (batch)
│   ├── load/                       # ClickHouse load jobs
│   │   ├── load_yfinance_daily.py
│   │   ├── load_yfinance_company.py
│   │   └── build_data_marts.py
│   └── schemas/                    # JSON schema definitions (raw & canonical)
│
├── infrastructure/                 # Python config modules
│   ├── spark_config.py             # Spark session with S3A/MinIO
│   ├── clickhouse_config.py        # ClickHouse client & DDL helpers
│   ├── minio_config.py             # MinIO client & upload helpers
│   ├── yfinance_api_config.py      # yfinance API with rate limiting
│   └── alpha_vantage_api_config.py # Alpha Vantage API config
│
├── airflow/
│   └── dags/
│       └── batch_dag.py            # yfinance_daily_extract_transform_load DAG
│
├── docker-compose.yml              # Full-stack orchestration (14 services)
├── Dockerfile.airflow              # Airflow image with Java + Python
├── requirements.txt                # Core Python dependencies
├── requirements-airflow.txt        # Airflow dependencies
└── .env                            # Environment configuration
```

---

## Data Flow

### Real-time Path
```
Finnhub WebSocket
    → extract_finhub_streaming.py
    → Kafka topic: stock_tick_raw
    → tranform_finnhub_streaming.py (PySpark Structured Streaming)
    → ClickHouse: fact_stock_tick + fact_stock_ohlc_1m
    → Spring Boot API (/api/stocks/tick)
    → Angular (60s polling)
```

### Batch Path
```
yfinance API
    → extract_yfinance_daily.py / extract_yfinance_company.py
    → MinIO: stock-raw/
    → Spark batch transform (Airflow DAG)
    → MinIO: stock-canonical/
    → load_yfinance_daily.py / load_yfinance_company.py
    → ClickHouse: fact_stock_daily + dim_company
    → Spring Boot API (/api/stocks, /api/companies)
    → Angular (on demand)
```

---

## Prerequisites

- Docker Desktop 24+ with Docker Compose V2
- 8 GB RAM minimum (16 GB recommended)
- Finnhub API key — [finnhub.io](https://finnhub.io)
- yfinance is installed as a Python package (no API key required)

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/hieunt/Real-time-Stock-Intelligence-Platform.git
cd Real-time-Stock-Intelligence-Platform
```

### 2. Configure environment variables

Edit the `.env` file and set your credentials:

```env
FINNHUB_API_KEY=your_key_here
MAIL_USERNAME=your_email@gmail.com
MAIL_PASSWORD=your_app_password
JWT_SECRET=your_jwt_secret_here
```

### 3. Start all services

```bash
docker compose up -d
```

This starts 14 containers. Wait ~60 seconds for all services to become healthy.

Check service status:

```bash
docker compose ps
```

### 4. Trigger the batch pipeline

Open Airflow at `http://localhost:8080` (admin / admin), then manually trigger the `yfinance_daily_extract_transform_load` DAG to load historical prices and company profiles into ClickHouse.

Or via CLI:

```bash
docker exec airflow-scheduler airflow dags trigger yfinance_daily_extract_transform_load
```

### 5. Start the backend (development)

```bash
cd app
mvn clean package -DskipTests
java -jar target/stock-*.jar
```

### 6. Start the frontend (development)

```bash
cd frontend
npm install
npm start
```

The app is available at `http://localhost:4200`.

---

## Service URLs

| Service | URL | Default Credentials |
|---|---|---|
| Angular Frontend | http://localhost:4200 | Register via UI |
| Spring Boot API | http://localhost:8081 | JWT |
| Swagger UI | http://localhost:8081/swagger-ui.html | — |
| Airflow UI | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8083 | — |
| MinIO Console | http://localhost:9002 | minioadmin / minioadmin |
| ClickHouse HTTP | http://localhost:8123 | — |

---

## API Documentation

Swagger UI: `http://localhost:8081/swagger-ui.html`

### Key Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/auth/register` | Register with OTP email verification |
| `POST` | `/api/auth/login` | Login, returns JWT access + refresh tokens |
| `POST` | `/api/auth/refresh` | Refresh access token |
| `GET` | `/api/stocks` | List all stocks with latest price |
| `GET` | `/api/stocks/{symbol}` | Stock detail + price history |
| `GET` | `/api/stocks/tick/{symbol}` | Latest real-time tick |
| `GET` | `/api/companies` | List company profiles |
| `GET` | `/api/companies/{symbol}` | Company detail |
| `GET` | `/api/portfolios` | User's portfolios |
| `POST` | `/api/portfolios` | Create portfolio |
| `POST` | `/api/portfolios/{id}/transactions` | Add buy/sell transaction |
| `GET` | `/api/watchlists` | User's watchlist |
| `POST` | `/api/watchlists` | Add ticker to watchlist |
| `DELETE` | `/api/watchlists/{symbol}` | Remove from watchlist |

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `FINNHUB_API_KEY` | Finnhub API key | — |
| `FINNHUB_SYMBOLS` | Comma-separated ticker list | `AAPL,MSFT,...` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `kafka:9092` |
| `CLICKHOUSE_HOST` | ClickHouse host | `clickhouse` |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port | `8123` |
| `CLICKHOUSE_DATABASE` | ClickHouse database name | `stocks` |
| `MINIO_ENDPOINT` | MinIO endpoint | `http://minio:9001` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `POSTGRES_HOST` | PostgreSQL host | `postgres` |
| `POSTGRES_DB` | PostgreSQL database | `airflow` |
| `POSTGRES_USER` | PostgreSQL user | `airflow` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `airflow` |
| `REDIS_HOST` | Redis host | `redis` |
| `JWT_SECRET` | JWT signing secret | — |
| `MAIL_HOST` | SMTP host | `smtp.gmail.com` |
| `MAIL_USERNAME` | Sender email address | — |
| `MAIL_PASSWORD` | Sender app password | — |

---

## Database Schema

### PostgreSQL (Transactional)

| Table | Purpose |
|---|---|
| `users` | User accounts |
| `roles` | Role definitions (ADMIN, ANALYST, USER) |
| `permissions` | Fine-grained permissions |
| `user_roles` | User ↔ Role mapping |
| `role_permissions` | Role ↔ Permission mapping |
| `portfolios` | User portfolio metadata |
| `portfolio_transactions` | Buy/sell transaction records |
| `watchlists` | User watchlist entries |

### ClickHouse (Analytics)

| Table | Engine | Purpose |
|---|---|---|
| `fact_stock_daily` | ReplacingMergeTree | Daily OHLCV with volatility metrics |
| `fact_stock_tick` | ReplacingMergeTree | Real-time trade ticks |
| `fact_stock_ohlc_1m` | ReplacingMergeTree | 1-minute OHLC candles |
| `dim_company` | ReplacingMergeTree | Company master data |

All ClickHouse tables are partitioned by date and ordered by `(symbol, timestamp)` for efficient time-series queries.
