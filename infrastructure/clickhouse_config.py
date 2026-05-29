from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
DDL_PATH = PROJECT_ROOT / "domain" / "stock" / "sql" / "ch.sql"
DIM_COMPANY_DDL_PATH = PROJECT_ROOT / "domain" / "stock" / "sql" / "dim_company.sql"
REALTIME_DDL_PATH = PROJECT_ROOT / "domain" / "stock" / "sql" / "real_time.sql"

load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    username: str
    password: str
    database: str
    table: str


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        return int(raw_value)
    except ValueError:
        return default


def get_clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=_get_int_env("CLICKHOUSE_HTTP_PORT", 8123),
        username=os.getenv("CLICKHOUSE_USER", "stock_user"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "stock_password"),
        database=os.getenv("CLICKHOUSE_DB", "stocks"),
        table=os.getenv("CLICKHOUSE_TABLE", "fact_stock_daily"),
    )


def get_clickhouse_client(config: ClickHouseConfig | None = None, database: str | None = None) -> Client:
    config = config or get_clickhouse_config()
    return get_client(
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        database=database or config.database,
    )


def ensure_database(config: ClickHouseConfig | None = None) -> None:
    config = config or get_clickhouse_config()
    client = get_clickhouse_client(config, database="default")
    client.command(f"CREATE DATABASE IF NOT EXISTS {config.database}")


def ensure_stock_daily_table(
    config: ClickHouseConfig | None = None,
    ddl_path: Path = DDL_PATH,
    table_name: str | None = None,
) -> None:
    config = config or get_clickhouse_config()
    target_table = table_name or config.table
    ensure_database(config)

    ddl = ddl_path.read_text(encoding="utf-8").strip().rstrip(";")
    ddl = ddl.replace("CREATE TABLE fact_stock_daily", f"CREATE TABLE IF NOT EXISTS {target_table}", 1)
    client = get_clickhouse_client(config)
    client.command(ddl)


def ensure_table_from_ddl(
    ddl_path: Path,
    default_table_name: str,
    table_name: str | None = None,
    config: ClickHouseConfig | None = None,
) -> None:
    config = config or get_clickhouse_config()
    target_table = table_name or default_table_name
    ensure_database(config)

    ddl = ddl_path.read_text(encoding="utf-8").strip().rstrip(";")
    ddl = ddl.replace(f"CREATE TABLE {default_table_name}", f"CREATE TABLE IF NOT EXISTS {target_table}", 1)
    client = get_clickhouse_client(config)
    client.command(ddl)


def ensure_dim_company_table(table_name: str | None = None, config: ClickHouseConfig | None = None) -> None:
    ensure_table_from_ddl(
        ddl_path=DIM_COMPANY_DDL_PATH,
        default_table_name="dim_company",
        table_name=table_name,
        config=config,
    )


def _split_sql_statements(sql_text: str) -> list[str]:
    statements = []
    current_statement = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue

        current_statement.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current_statement).strip().rstrip(";")
            if statement:
                statements.append(statement)
            current_statement = []

    if current_statement:
        statements.append("\n".join(current_statement).strip().rstrip(";"))

    return statements


def ensure_realtime_tables(config: ClickHouseConfig | None = None, ddl_path: Path = REALTIME_DDL_PATH) -> None:
    config = config or get_clickhouse_config()
    ensure_database(config)
    client = get_clickhouse_client(config)

    for statement in _split_sql_statements(ddl_path.read_text(encoding="utf-8")):
        statement = statement.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
        client.command(statement)


def insert_dataframe(
    df: pd.DataFrame,
    table_name: str | None = None,
    config: ClickHouseConfig | None = None,
) -> int:
    config = config or get_clickhouse_config()
    target_table = table_name or config.table

    if df.empty:
        return 0

    client = get_clickhouse_client(config)
    client.insert_df(target_table, df)
    return len(df)


def optimize_table_final(
    table_name: str | None = None,
    config: ClickHouseConfig | None = None,
) -> None:
    config = config or get_clickhouse_config()
    target_table = table_name or config.table
    client = get_clickhouse_client(config)
    client.command(f"OPTIMIZE TABLE {target_table} FINAL")
