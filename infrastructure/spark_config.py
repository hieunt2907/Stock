from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from infrastructure.minio_config import get_minio_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class SparkConfig:
    master: str
    sql_shuffle_partitions: int
    default_parallelism: int
    driver_memory: str
    driver_cores: int
    executor_memory: str
    executor_cores: int
    executor_instances: int
    jars_packages: str


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        return int(raw_value)
    except ValueError:
        return default


def get_spark_config() -> SparkConfig:
    return SparkConfig(
        master=os.getenv("SPARK_MASTER", "local[*]"),
        sql_shuffle_partitions=_get_int_env("SPARK_SQL_SHUFFLE_PARTITIONS", 8),
        default_parallelism=_get_int_env("SPARK_DEFAULT_PARALLELISM", 8),
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        driver_cores=_get_int_env("SPARK_DRIVER_CORES", 1),
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        executor_cores=_get_int_env("SPARK_EXECUTOR_CORES", 2),
        executor_instances=_get_int_env("SPARK_EXECUTOR_INSTANCES", 1),
        jars_packages=os.getenv("SPARK_JARS_PACKAGES", "org.apache.hadoop:hadoop-aws:3.4.1"),
    )


def create_spark_session(app_name: str, config: SparkConfig | None = None) -> SparkSession:
    spark_config = config or get_spark_config()
    minio_config = get_minio_config()

    return (
        SparkSession.builder.appName(app_name)
        .master(spark_config.master)
        .config("spark.sql.shuffle.partitions", str(spark_config.sql_shuffle_partitions))
        .config("spark.default.parallelism", str(spark_config.default_parallelism))
        .config("spark.driver.memory", spark_config.driver_memory)
        .config("spark.driver.cores", str(spark_config.driver_cores))
        .config("spark.executor.memory", spark_config.executor_memory)
        .config("spark.executor.cores", str(spark_config.executor_cores))
        .config("spark.executor.instances", str(spark_config.executor_instances))
        .config("spark.jars.packages", spark_config.jars_packages)
        .config("spark.hadoop.fs.s3a.endpoint", minio_config.spark_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_config.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_config.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio_config.secure).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
