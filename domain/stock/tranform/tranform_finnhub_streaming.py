from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = PROJECT_ROOT / ".env"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

load_dotenv(ENV_PATH)

from infrastructure.clickhouse_config import ensure_realtime_tables, insert_dataframe, optimize_table_final
from infrastructure.minio_config import ensure_bucket, get_minio_client, get_minio_config
from infrastructure.spark_config import create_spark_session


DEFAULT_INPUT_TOPIC = os.getenv("STOCK_TICK_RAW_TOPIC", "stock_tick_raw")
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TICK_TABLE = "fact_stock_tick"
OHLC_TABLE = "fact_stock_ohlc_1m"

TICK_COLUMNS = [
    "event_id",
    "symbol",
    "event_time",
    "price",
    "volume",
    "source",
    "ingestion_time",
    "version",
]

OHLC_COLUMNS = [
    "symbol",
    "window_start",
    "window_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
    "source",
    "ingestion_time",
    "version",
]

RAW_TICK_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("trade_timestamp", StringType(), True),
        StructField("conditions", ArrayType(StringType()), True),
        StructField("source", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField(
            "raw_payload",
            StructType(
                [
                    StructField("p", DoubleType(), True),
                    StructField("s", StringType(), True),
                    StructField("t", LongType(), True),
                    StructField("v", DoubleType(), True),
                    StructField("c", ArrayType(StringType()), True),
                ]
            ),
            True,
        ),
    ]
)


def ensure_checkpoint_bucket(checkpoint_location: str) -> None:
    if not checkpoint_location.startswith("s3a://"):
        return

    minio_config = get_minio_config()
    minio_client = get_minio_client(minio_config)
    bucket_name = checkpoint_location.replace("s3a://", "", 1).split("/", 1)[0]
    ensure_bucket(minio_client, bucket_name)


def transform_raw_ticks(raw_stream_df: DataFrame) -> DataFrame:
    parsed_df = raw_stream_df.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_json"),
    ).select(
        "kafka_timestamp",
        "raw_json",
        F.from_json(F.col("raw_json"), RAW_TICK_SCHEMA).alias("event"),
    )

    flattened_df = parsed_df.select(
        F.coalesce(F.col("event.symbol"), F.col("event.raw_payload.s")).alias("raw_symbol"),
        F.coalesce(F.col("event.price"), F.col("event.raw_payload.p")).cast("double").alias("price"),
        F.coalesce(F.col("event.volume"), F.col("event.raw_payload.v")).cast("long").alias("volume"),
        F.col("event.trade_timestamp").alias("trade_timestamp"),
        F.col("event.raw_payload.t").alias("trade_timestamp_ms"),
        F.coalesce(F.col("event.source"), F.lit("finnhub")).alias("source"),
        F.col("event.ingested_at").alias("raw_ingested_at"),
        F.col("kafka_timestamp"),
    )

    standardized_df = flattened_df.select(
        F.when(F.instr(F.col("raw_symbol"), ":") > 0, F.split(F.col("raw_symbol"), ":").getItem(1))
        .otherwise(F.col("raw_symbol"))
        .alias("symbol"),
        F.coalesce(
            F.to_timestamp("trade_timestamp"),
            F.to_timestamp(F.from_unixtime((F.col("trade_timestamp_ms") / F.lit(1000)).cast("long"))),
        ).alias("event_time"),
        F.col("price"),
        F.col("volume"),
        F.col("source"),
        F.coalesce(F.to_timestamp("raw_ingested_at"), F.col("kafka_timestamp"), F.current_timestamp()).alias("ingestion_time"),
    )

    valid_df = standardized_df.filter(
        (F.col("symbol").isNotNull())
        & (F.length(F.col("symbol")) > 0)
        & (F.col("event_time").isNotNull())
        & (F.col("price") > 0)
        & (F.col("volume") >= 0)
        & (F.col("source").isNotNull())
        & (F.col("ingestion_time").isNotNull())
    )

    return valid_df.select(
        F.sha2(
            F.concat_ws(
                "|",
                F.col("symbol"),
                F.col("event_time").cast("string"),
                F.col("price").cast("string"),
                F.col("volume").cast("string"),
                F.col("source"),
            ),
            256,
        ).alias("event_id"),
        "symbol",
        "event_time",
        "price",
        "volume",
        "source",
        "ingestion_time",
        F.unix_timestamp("ingestion_time").cast("long").alias("version"),
    ).select(*TICK_COLUMNS)


def build_ohlc_1m(tick_df: DataFrame, watermark: str) -> DataFrame:
    return (
        tick_df.withWatermark("event_time", watermark)
        .groupBy(
            F.col("symbol"),
            F.col("source"),
            F.window(F.col("event_time"), "1 minute").alias("event_window"),
        )
        .agg(
            F.min_by(F.col("price"), F.col("event_time")).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by(F.col("price"), F.col("event_time")).alias("close"),
            F.sum("volume").cast("long").alias("volume"),
            F.sum(F.col("price") * F.col("volume")).alias("value"),
            F.max("ingestion_time").alias("ingestion_time"),
        )
        .select(
            "symbol",
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
            "source",
            "ingestion_time",
            F.unix_timestamp("ingestion_time").cast("long").alias("version"),
        )
        .select(*OHLC_COLUMNS)
    )


def insert_batch(df: DataFrame, batch_id: int, table_name: str, optimize_final: bool) -> None:
    if df.isEmpty():
        print(f"[{table_name}] batch={batch_id} empty")
        return

    inserted_count = insert_dataframe(df.toPandas(), table_name=table_name)
    print(f"[{table_name}] batch={batch_id} inserted={inserted_count}")

    if optimize_final:
        optimize_table_final(table_name=table_name)
        print(f"[{table_name}] batch={batch_id} optimized FINAL")


def run_streaming_transform(
    bootstrap_servers: str,
    input_topic: str,
    checkpoint_location: str,
    starting_offsets: str,
    processing_time: str,
    watermark: str,
    optimize_final: bool,
) -> None:
    ensure_realtime_tables()
    ensure_checkpoint_bucket(checkpoint_location)

    spark = create_spark_session(app_name="transform-finnhub-streaming-clickhouse")
    spark.sparkContext.setLogLevel("WARN")

    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    tick_df = transform_raw_ticks(raw_stream_df=raw_stream_df)
    ohlc_df = build_ohlc_1m(tick_df=tick_df, watermark=watermark)

    tick_query = (
        tick_df.writeStream.foreachBatch(
            lambda df, batch_id: insert_batch(df, batch_id, TICK_TABLE, optimize_final)
        )
        .option("checkpointLocation", f"{checkpoint_location.rstrip('/')}/{TICK_TABLE}")
        .outputMode("append")
        .trigger(processingTime=processing_time)
        .start()
    )

    ohlc_query = (
        ohlc_df.writeStream.foreachBatch(
            lambda df, batch_id: insert_batch(df, batch_id, OHLC_TABLE, optimize_final)
        )
        .option("checkpointLocation", f"{checkpoint_location.rstrip('/')}/{OHLC_TABLE}")
        .outputMode("update")
        .trigger(processingTime=processing_time)
        .start()
    )

    spark.streams.awaitAnyTermination()
    tick_query.stop()
    ohlc_query.stop()


def main() -> None:
    minio_config = get_minio_config()
    default_checkpoint = f"s3a://{minio_config.canonical_bucket}/checkpoints/realtime_clickhouse"

    parser = argparse.ArgumentParser(
        description="Transform Finnhub raw trades from Kafka and load realtime facts directly into ClickHouse."
    )
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--input-topic", default=DEFAULT_INPUT_TOPIC)
    parser.add_argument("--checkpoint-location", default=os.getenv("STOCK_TICK_CHECKPOINT_LOCATION", default_checkpoint))
    parser.add_argument("--starting-offsets", default=os.getenv("STOCK_TICK_STARTING_OFFSETS", "latest"))
    parser.add_argument("--processing-time", default=os.getenv("STOCK_TICK_PROCESSING_TIME", "10 seconds"))
    parser.add_argument("--watermark", default=os.getenv("STOCK_TICK_WATERMARK", "2 minutes"))
    parser.add_argument("--optimize-final", action="store_true", help="Run OPTIMIZE TABLE ... FINAL after every batch.")
    args = parser.parse_args()

    run_streaming_transform(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        checkpoint_location=args.checkpoint_location,
        starting_offsets=args.starting_offsets,
        processing_time=args.processing_time,
        watermark=args.watermark,
        optimize_final=args.optimize_final,
    )


if __name__ == "__main__":
    main()
