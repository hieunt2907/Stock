from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from infrastructure.minio_config import ensure_bucket, get_minio_client, get_minio_config
from infrastructure.spark_config import create_spark_session


FINAL_COLUMNS = [
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
    return f"s3a://{minio_config.raw_bucket}/raw/yfinance/daily/*/*.json"


def default_output_path() -> str:
    minio_config = get_minio_config()
    return f"s3a://{minio_config.canonical_bucket}/stock_daily_price/"


def standardize_columns(df: DataFrame) -> DataFrame:
    standardized = df
    for column_name in df.columns:
        normalized_name = column_name.strip().lower().replace(" ", "_")
        standardized = standardized.withColumnRenamed(column_name, normalized_name)

    rename_map = {
        "date": "trading_date",
        "ticker": "symbol",
        "adj_close": "adjusted_close",
    }
    for old_name, new_name in rename_map.items():
        if old_name in standardized.columns and new_name not in standardized.columns:
            standardized = standardized.withColumnRenamed(old_name, new_name)

    required_columns = ["symbol", "trading_date", "open", "high", "low", "close", "volume"]
    missing_columns = [column for column in required_columns if column not in standardized.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return standardized


def cast_data_types(df: DataFrame) -> DataFrame:
    casted = (
        df.withColumn("symbol", F.col("symbol").cast(T.StringType()))
        .withColumn("trading_date", F.to_date(F.col("trading_date")))
        .withColumn("open", F.col("open").cast(T.DoubleType()))
        .withColumn("high", F.col("high").cast(T.DoubleType()))
        .withColumn("low", F.col("low").cast(T.DoubleType()))
        .withColumn("close", F.col("close").cast(T.DoubleType()))
        .withColumn("volume", F.col("volume").cast(T.LongType()))
    )

    if "ingestion_time" in casted.columns:
        return casted.withColumn("ingestion_time", F.to_timestamp(F.col("ingestion_time")))

    return casted.withColumn("ingestion_time", F.current_timestamp())


def validate_data_quality(df: DataFrame) -> DataFrame:
    quality_condition = (
        F.col("symbol").isNotNull()
        & F.col("trading_date").isNotNull()
        & (F.col("open") > 0)
        & (F.col("high") > 0)
        & (F.col("low") > 0)
        & (F.col("close") > 0)
        & (F.col("volume") >= 0)
        & (F.col("high") >= F.col("low"))
        & (F.col("high") >= F.col("open"))
        & (F.col("high") >= F.col("close"))
        & (F.col("low") <= F.col("open"))
        & (F.col("low") <= F.col("close"))
    )
    return df.filter(quality_condition)


def calculate_derived_metrics(df: DataFrame) -> DataFrame:
    daily_window = Window.partitionBy("symbol").orderBy("trading_date")

    return (
        df.withColumn("value", ((F.col("open") + F.col("high") + F.col("low") + F.col("close")) / F.lit(4)) * F.col("volume"))
        .withColumn("previous_close", F.lag("close").over(daily_window))
        .withColumn(
            "daily_return",
            F.when(F.col("previous_close").isNull() | (F.col("previous_close") == 0), F.lit(None).cast(T.DoubleType()))
            .otherwise((F.col("close") - F.col("previous_close")) / F.col("previous_close")),
        )
        .withColumn("volatility", (F.col("high") - F.col("low")) / F.col("open"))
        .drop("previous_close")
    )


def deduplicate_latest(df: DataFrame) -> DataFrame:
    dedupe_window = Window.partitionBy("symbol", "trading_date").orderBy(F.col("ingestion_time").desc())
    return (
        df.withColumn("row_number", F.row_number().over(dedupe_window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def add_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("source", F.lit("yfinance"))
        .withColumn("version", F.unix_timestamp(F.col("ingestion_time")).cast(T.LongType()))
    )


def transform_yfinance_daily(df: DataFrame) -> DataFrame:
    standardized = standardize_columns(df)
    casted = cast_data_types(standardized)
    valid = validate_data_quality(casted)
    metrics = calculate_derived_metrics(valid)
    deduped = deduplicate_latest(metrics)
    with_metadata = add_metadata(deduped)
    return with_metadata.select(*FINAL_COLUMNS)


def run_transform(input_path: str, output_path: str, mode: str = "overwrite") -> None:
    minio_config = get_minio_config()
    minio_client = get_minio_client(minio_config)
    ensure_bucket(minio_client, minio_config.canonical_bucket)

    spark = create_spark_session(app_name="transform-yfinance-daily")
    try:
        raw_df = spark.read.json(input_path)
        transformed_df = transform_yfinance_daily(raw_df)
        transformed_count = transformed_df.count()
        transformed_df.write.mode(mode).parquet(output_path)
        print(f"Transformed {transformed_count} rows")
        print(f"Output written to {output_path}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform yfinance daily JSON data from raw MinIO to canonical Parquet MinIO.")
    parser.add_argument("--input-path", default=default_input_path(), help="Input s3a path or glob for raw yfinance JSON files.")
    parser.add_argument("--output-path", default=default_output_path(), help="Output s3a path for canonical parquet files.")
    parser.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "error", "ignore"], help="Spark write mode.")
    args = parser.parse_args()

    run_transform(input_path=args.input_path, output_path=args.output_path, mode=args.mode)


if __name__ == "__main__":
    main()
