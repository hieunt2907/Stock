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
    return f"s3a://{minio_config.raw_bucket}/raw/yfinance/company_profile/*/*.json"


def default_output_path() -> str:
    minio_config = get_minio_config()
    return f"s3a://{minio_config.canonical_bucket}/dim_company/"


def standardize_columns(df: DataFrame) -> DataFrame:
    standardized = df
    for column_name in df.columns:
        normalized_name = column_name.strip().lower().replace(" ", "_")
        standardized = standardized.withColumnRenamed(column_name, normalized_name)

    required_columns = ["symbol", "company_name", "source", "updated_at"]
    missing_columns = [column for column in required_columns if column not in standardized.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return standardized


def cast_data_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("symbol", F.upper(F.col("symbol").cast(T.StringType())))
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
    )


def validate_data_quality(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("symbol").isNotNull()
        & (F.length(F.col("symbol")) > 0)
        & F.col("company_name").isNotNull()
        & (F.length(F.col("company_name")) > 0)
        & F.col("updated_at").isNotNull()
    )


def deduplicate_latest(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("symbol").orderBy(F.col("updated_at").desc())
    return df.withColumn("row_number", F.row_number().over(window)).filter(F.col("row_number") == 1).drop("row_number")


def add_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("version", F.unix_timestamp(F.col("updated_at")).cast(T.LongType()))


def transform_yfinance_company(df: DataFrame) -> DataFrame:
    standardized = standardize_columns(df)
    casted = cast_data_types(standardized)
    valid = validate_data_quality(casted)
    deduped = deduplicate_latest(valid)
    with_metadata = add_metadata(deduped)
    return with_metadata.select(*FINAL_COLUMNS)


def run_transform(input_path: str, output_path: str, mode: str = "overwrite") -> None:
    minio_config = get_minio_config()
    minio_client = get_minio_client(minio_config)
    ensure_bucket(minio_client, minio_config.canonical_bucket)

    spark = create_spark_session(app_name="transform-yfinance-company")
    try:
        raw_df = spark.read.json(input_path)
        transformed_df = transform_yfinance_company(raw_df)
        transformed_count = transformed_df.count()
        transformed_df.write.mode(mode).parquet(output_path)
        print(f"Transformed {transformed_count} company rows")
        print(f"Output written to {output_path}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform yfinance company profile JSON from raw MinIO to canonical Parquet MinIO.")
    parser.add_argument("--input-path", default=default_input_path(), help="Input s3a path or glob for raw company profile JSON files.")
    parser.add_argument("--output-path", default=default_output_path(), help="Output s3a path for canonical company parquet files.")
    parser.add_argument("--mode", default="overwrite", choices=["append", "overwrite", "error", "ignore"], help="Spark write mode.")
    args = parser.parse_args()

    run_transform(input_path=args.input_path, output_path=args.output_path, mode=args.mode)


if __name__ == "__main__":
    main()
