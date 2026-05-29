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

from infrastructure.clickhouse_config import ensure_dim_company_table, insert_dataframe, optimize_table_final
from infrastructure.minio_config import get_minio_config
from infrastructure.spark_config import create_spark_session


CLICKHOUSE_COLUMNS = [
    "symbol",
    "company_name",
    "exchange",
    "industry",
    "sector",
    "country",
    "currency",
    "website",
    "description",
    "market_cap",
    "shares_outstanding",
    "source",
    "updated_at",
    "version",
]


def default_input_path() -> str:
    minio_config = get_minio_config()
    return f"s3a://{minio_config.canonical_bucket}/dim_company/"


def prepare_for_clickhouse(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("symbol", F.col("symbol").cast(T.StringType()))
        .withColumn("company_name", F.col("company_name").cast(T.StringType()))
        .withColumn("exchange", F.coalesce(F.col("exchange").cast(T.StringType()), F.lit("")))
        .withColumn("industry", F.coalesce(F.col("industry").cast(T.StringType()), F.lit("")))
        .withColumn("sector", F.coalesce(F.col("sector").cast(T.StringType()), F.lit("")))
        .withColumn("country", F.coalesce(F.col("country").cast(T.StringType()), F.lit("")))
        .withColumn("currency", F.coalesce(F.col("currency").cast(T.StringType()), F.lit("")))
        .withColumn("website", F.coalesce(F.col("website").cast(T.StringType()), F.lit("")))
        .withColumn("description", F.coalesce(F.col("description").cast(T.StringType()), F.lit("")))
        .withColumn("market_cap", F.coalesce(F.col("market_cap").cast(T.DoubleType()), F.lit(0.0)))
        .withColumn("shares_outstanding", F.coalesce(F.col("shares_outstanding").cast(T.DoubleType()), F.lit(0.0)))
        .withColumn("source", F.coalesce(F.col("source").cast(T.StringType()), F.lit("yfinance")))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        .withColumn("version", F.col("version").cast(T.LongType()))
        .select(*CLICKHOUSE_COLUMNS)
    )


def load_to_clickhouse(input_path: str, table_name: str = "dim_company") -> None:
    spark = create_spark_session(app_name="load-yfinance-company-clickhouse")
    try:
        canonical_df = spark.read.option("recursiveFileLookup", "true").parquet(input_path)
        clickhouse_df = prepare_for_clickhouse(canonical_df)
        row_count = clickhouse_df.count()

        ensure_dim_company_table(table_name=table_name)
        inserted_count = insert_dataframe(clickhouse_df.toPandas(), table_name=table_name)
        optimize_table_final(table_name=table_name)

        print(f"Read {row_count} company rows from {input_path}")
        print(f"Inserted {inserted_count} rows into ClickHouse table {table_name}")
        print(f"Optimized ClickHouse table {table_name} FINAL")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load canonical yfinance company profile parquet data into ClickHouse.")
    parser.add_argument("--input-path", default=default_input_path(), help="Input s3a path for canonical company parquet files.")
    parser.add_argument("--table", default="dim_company", help="ClickHouse target table.")
    args = parser.parse_args()

    load_to_clickhouse(input_path=args.input_path, table_name=args.table)


if __name__ == "__main__":
    main()
