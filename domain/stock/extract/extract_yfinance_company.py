from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from infrastructure.minio_config import get_minio_config, upload_dataframe_as_json
from infrastructure.yfinance_api_config import yfinance_rate_limiter


OUTPUT_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "raw" / "yfinance_company_profile_raw.json"

OUTPUT_COLUMNS = [
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
]

DEFAULT_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "GOOG",
    "META",
    "AMZN",
    "AMD",
    "INTC",
    "TSM",
    "JPM",
    "BAC",
    "GS",
    "V",
    "WMT",
    "COST",
    "NFLX",
    "DIS",
    "TSLA",
    "RIVN",
    "SPY",
]
DEFAULT_MINIO_PREFIX = "raw/yfinance/company_profile"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_schema(schema_path: Path = OUTPUT_SCHEMA_PATH) -> dict:
    with schema_path.open("r", encoding="utf-8") as schema_file:
        return json.load(schema_file)


def validate_company_output_schema(df: pd.DataFrame, schema: dict | None = None) -> None:
    schema = schema or load_schema()
    required_columns = schema.get("required", [])
    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(f"yfinance company profile schema missing columns: {missing_columns}")

    expected_columns = list(schema.get("properties", {}).keys())
    if expected_columns and list(df.columns) != expected_columns:
        raise ValueError(
            "yfinance company profile columns do not match schema: "
            f"expected {expected_columns}, got {list(df.columns)}"
        )


def _safe_number(value: object) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_yfinance_company_profile(symbol: str) -> dict:
    yfinance_rate_limiter.wait()
    try:
        info = yf.Ticker(symbol).info or {}
    finally:
        yfinance_rate_limiter.mark_request()

    return {
        "symbol": symbol,
        "company_name": info.get("longName") or info.get("shortName") or symbol,
        "exchange": info.get("exchange") or info.get("fullExchangeName"),
        "industry": info.get("industry"),
        "sector": info.get("sector"),
        "country": info.get("country"),
        "currency": info.get("currency") or info.get("financialCurrency"),
        "website": info.get("website"),
        "description": info.get("longBusinessSummary"),
        "market_cap": _safe_number(info.get("marketCap")),
        "shares_outstanding": _safe_number(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")),
        "source": "yfinance",
        "updated_at": _utc_now_iso(),
    }


def fetch_many_yfinance_company_profiles(symbols: Iterable[str]) -> pd.DataFrame:
    records = []
    tickers = [symbol.strip().upper() for symbol in symbols if symbol.strip()]

    for ticker in tickers:
        try:
            records.append(fetch_yfinance_company_profile(ticker))
            print(f"[{ticker}] fetched company profile")
        except Exception as exc:
            print(f"[{ticker}] error: {exc}")

    if not records:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df_profiles = pd.DataFrame(records)
    df_profiles = df_profiles[OUTPUT_COLUMNS]
    validate_company_output_schema(df_profiles)
    return df_profiles


def _parse_symbols(raw_symbols: str) -> list[str]:
    return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]


def _build_minio_object_name(symbols: list[str]) -> str:
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    symbol_part = "multi" if len(symbols) > 1 else symbols[0].lower()
    return f"{DEFAULT_MINIO_PREFIX}/run_date={run_date}/{symbol_part}_{run_date}.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract company profiles from yfinance.")
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated tickers. Default is the configured 20-symbol US stock/ETF universe.",
    )
    parser.add_argument("--bucket", default=get_minio_config().raw_bucket, help="MinIO bucket for raw JSON output.")
    parser.add_argument("--object-name", help="Optional MinIO object path. Default is partitioned by run_date.")
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols)
    df_profiles = fetch_many_yfinance_company_profiles(symbols)
    object_name = args.object_name or _build_minio_object_name(symbols)
    object_uri = upload_dataframe_as_json(df_profiles, object_name=object_name, bucket_name=args.bucket)
    print(f"Saved {len(df_profiles)} rows to {object_uri}")


if __name__ == "__main__":
    main()
