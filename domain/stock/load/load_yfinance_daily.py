from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from infrastructure.clickhouse_config import (
    ensure_stock_daily_table,
    get_clickhouse_config,
    insert_dataframe,
    optimize_table_final,
)
from infrastructure.minio_config import get_minio_config
from infrastructure.spark_config import create_spark_session


CLICKHOUSE_COLUMNS = [
    "symbol",
    "trading_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
    "daily_return",
    "volatility",
    "source",
    "version",
]


def default_input_path() -> str:
    minio_config = get_minio_config()
    return f"s3a://{minio_config.canonical_bucket}/stock_daily_price/"


def prepare_for_clickhouse(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("symbol", F.col("symbol").cast(T.StringType()))
        .withColumn("trading_date", F.to_date(F.col("trading_date")))
        .withColumn("open", F.col("open").cast(T.DoubleType()))
        .withColumn("high", F.col("high").cast(T.DoubleType()))
        .withColumn("low", F.col("low").cast(T.DoubleType()))
        .withColumn("close", F.col("close").cast(T.DoubleType()))
        .withColumn("volume", F.col("volume").cast(T.LongType()))
        .withColumn("value", F.col("value").cast(T.DoubleType()))
        .withColumn("daily_return", F.coalesce(F.col("daily_return"), F.lit(0.0)).cast(T.DoubleType()))
        .withColumn("volatility", F.coalesce(F.col("volatility"), F.lit(0.0)).cast(T.DoubleType()))
        .withColumn("source", F.col("source").cast(T.StringType()))
        .withColumn("version", F.col("version").cast(T.LongType()))
        .select(*CLICKHOUSE_COLUMNS)
    )


def load_to_clickhouse(input_path: str, table_name: str | None = None) -> None:
    spark = create_spark_session(app_name="load-yfinance-daily-clickhouse")
    try:
        canonical_df = spark.read.option("recursiveFileLookup", "true").parquet(input_path)
        clickhouse_df = prepare_for_clickhouse(canonical_df)
        row_count = clickhouse_df.count()

        ensure_stock_daily_table(table_name=table_name)
        inserted_count = insert_dataframe(clickhouse_df.toPandas(), table_name=table_name)
        target_table = table_name or get_clickhouse_config().table
        optimize_table_final(table_name=target_table)

        print(f"Read {row_count} rows from {input_path}")
        print(f"Inserted {inserted_count} rows into ClickHouse table {target_table}")
        print(f"Optimized ClickHouse table {target_table} FINAL")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load canonical yfinance daily parquet data into ClickHouse.")
    parser.add_argument("--input-path", default=default_input_path(), help="Input s3a path for canonical parquet files.")
    parser.add_argument("--table", default=get_clickhouse_config().table, help="ClickHouse target table.")
    args = parser.parse_args()

    load_to_clickhouse(input_path=args.input_path, table_name=args.table)


if __name__ == "__main__":
    main()
