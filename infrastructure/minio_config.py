from __future__ import annotations

import os
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath

import pandas as pd
from dotenv import load_dotenv
from minio import Minio


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    spark_endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    raw_bucket: str
    canonical_bucket: str


def get_minio_config() -> MinioConfig:
    return MinioConfig(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9001"),
        spark_endpoint=os.getenv("MINIO_SPARK_ENDPOINT", "http://minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
        secret_key=os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        raw_bucket=os.getenv("MINIO_RAW_BUCKET", "stock-raw"),
        canonical_bucket=os.getenv("MINIO_CANONICAL_BUCKET", "stock-canonical"),
    )


def get_minio_client(config: MinioConfig | None = None) -> Minio:
    config = config or get_minio_config()
    return Minio(
        endpoint=config.endpoint,
        access_key=config.access_key,
        secret_key=config.secret_key,
        secure=config.secure,
    )


def ensure_bucket(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def upload_dataframe_as_parquet(
    df: pd.DataFrame,
    object_name: str,
    bucket_name: str | None = None,
    client: Minio | None = None,
) -> str:
    config = get_minio_config()
    bucket = bucket_name or config.raw_bucket
    minio_client = client or get_minio_client(config)
    ensure_bucket(minio_client, bucket)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    normalized_object_name = str(PurePosixPath(object_name))
    minio_client.put_object(
        bucket_name=bucket,
        object_name=normalized_object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    return f"s3://{bucket}/{normalized_object_name}"


def upload_dataframe_as_json(
    df: pd.DataFrame,
    object_name: str,
    bucket_name: str | None = None,
    client: Minio | None = None,
) -> str:
    config = get_minio_config()
    bucket = bucket_name or config.raw_bucket
    minio_client = client or get_minio_client(config)
    ensure_bucket(minio_client, bucket)

    payload = df.to_json(orient="records", lines=True, date_format="iso").encode("utf-8")
    buffer = BytesIO(payload)

    normalized_object_name = str(PurePosixPath(object_name))
    minio_client.put_object(
        bucket_name=bucket,
        object_name=normalized_object_name,
        data=buffer,
        length=len(payload),
        content_type="application/x-ndjson",
    )

    return f"s3://{bucket}/{normalized_object_name}"
