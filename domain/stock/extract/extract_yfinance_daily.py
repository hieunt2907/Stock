from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from infrastructure.yfinance_api_config import yfinance_rate_limiter
from infrastructure.minio_config import get_minio_config, upload_dataframe_as_json

OUTPUT_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "raw" / "yfinance_daily_raw.json"


OUTPUT_COLUMNS = [
    "symbol",
    "trading_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
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
DEFAULT_PERIOD = "5d"
DEFAULT_HISTORICAL_START_DATE = "2021-01-01"
DEFAULT_MINIO_PREFIX = "raw/yfinance/daily"


def _default_start_date(days_back: int = 2) -> str:
    return (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")


def _default_end_date() -> str:
    return date.today().strftime("%Y-%m-%d")


def _resolve_end_date(end_date: str | None) -> str:
    if end_date is None or end_date.lower() == "now":
        return _default_end_date()

    return end_date


def load_schema(schema_path: Path = OUTPUT_SCHEMA_PATH) -> dict:
    with schema_path.open("r", encoding="utf-8") as schema_file:
        return json.load(schema_file)


def validate_daily_output_schema(df: pd.DataFrame, schema: dict | None = None) -> None:
    schema = schema or load_schema()
    required_columns = schema.get("required", [])
    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(f"yfinance daily output schema missing columns: {missing_columns}")

    expected_columns = list(schema.get("properties", {}).keys())
    if expected_columns and list(df.columns) != expected_columns:
        raise ValueError(
            "yfinance daily output columns do not match schema: "
            f"expected {expected_columns}, got {list(df.columns)}"
        )


def _normalize_yfinance_frame(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    normalized = df.copy()
    normalized["trading_date"] = pd.to_datetime(normalized["trading_date"]).dt.strftime("%Y-%m-%d")

    numeric_columns = ["open", "high", "low", "close", "volume", "value"]
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["volume"] = normalized["volume"].astype("Int64")
    normalized["symbol"] = symbol

    return normalized[OUTPUT_COLUMNS].sort_values(["symbol", "trading_date"]).reset_index(drop=True)


def fetch_yfinance_daily(
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    period: str = DEFAULT_PERIOD,
) -> pd.DataFrame:
    yfinance_rate_limiter.wait()
    try:
        ticker = yf.Ticker(symbol)
        if start_date or end_date:
            start = start_date or _default_start_date()
            end = _resolve_end_date(end_date)
            yfinance_end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            df_history = ticker.history(start=start, end=yfinance_end, interval="1d", auto_adjust=False)
        else:
            df_history = ticker.history(period=period, interval="1d", auto_adjust=False)
    finally:
        yfinance_rate_limiter.mark_request()

    if df_history.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    if isinstance(df_history.columns, pd.MultiIndex):
        df_history.columns = df_history.columns.get_level_values(0)

    df_history = df_history.reset_index()
    df_history = df_history.rename(
        columns={
            "Date": "trading_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    df_history["symbol"] = symbol
    df_history["value"] = (
        (df_history["open"] + df_history["high"] + df_history["low"] + df_history["close"]) / 4
    ) * df_history["volume"]

    df_daily = _normalize_yfinance_frame(df_history[OUTPUT_COLUMNS], symbol)
    if start_date or end_date:
        start = start_date or _default_start_date()
        end = _resolve_end_date(end_date)
        df_daily = df_daily[
            (df_daily["trading_date"] >= start)
            & (df_daily["trading_date"] <= end)
        ].reset_index(drop=True)

    if not df_daily.empty:
        validate_daily_output_schema(df_daily)

    return df_daily


def fetch_many_yfinance_daily(
    symbols: Iterable[str],
    start_date: str | None = None,
    end_date: str | None = None,
    period: str = DEFAULT_PERIOD,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    tickers = [symbol.strip().upper() for symbol in symbols if symbol.strip()]

    for ticker in tickers:
        try:
            df_daily = fetch_yfinance_daily(
                symbol=ticker,
                start_date=start_date,
                end_date=end_date,
                period=period,
            )
            if not df_daily.empty:
                frames.append(df_daily)
                print(f"[{ticker}] fetched {len(df_daily)} rows")
            else:
                print(f"[{ticker}] no data")
        except Exception as exc:
            print(f"[{ticker}] error: {exc}")

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return pd.concat(frames, ignore_index=True)


def _parse_symbols(raw_symbols: str) -> list[str]:
    return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]


def _build_minio_object_name(symbols: list[str], start_date: str | None, end_date: str | None, period: str) -> str:
    run_date = date.today().strftime("%Y-%m-%d")
    symbol_part = "multi" if len(symbols) > 1 else symbols[0].lower()

    if start_date or end_date:
        start_part = start_date or _default_start_date()
        end_part = _resolve_end_date(end_date)
        file_name = f"{symbol_part}_{start_part}_{end_part}.json"
    else:
        file_name = f"{symbol_part}_{period}_{run_date}.json"

    return f"{DEFAULT_MINIO_PREFIX}/run_date={run_date}/{file_name}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract daily historical price data from yfinance.")
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated tickers. Default is the configured 20-symbol US stock/ETF universe.",
    )
    parser.add_argument("--period", default=DEFAULT_PERIOD, help="yfinance period used when dates are not provided.")
    parser.add_argument("--start-date", default=DEFAULT_HISTORICAL_START_DATE, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", default="now", help="End date in YYYY-MM-DD format, or now.")
    parser.add_argument("--bucket", default=get_minio_config().raw_bucket, help="MinIO bucket for raw JSON output.")
    parser.add_argument("--object-name", help="Optional MinIO object path. Default is partitioned by run_date.")
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols)
    df_daily = fetch_many_yfinance_daily(
        symbols=symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        period=args.period,
    )

    object_name = args.object_name or _build_minio_object_name(
        symbols=symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        period=args.period,
    )
    object_uri = upload_dataframe_as_json(df_daily, object_name=object_name, bucket_name=args.bucket)
    print(f"Saved {len(df_daily)} rows to {object_uri}")


if __name__ == "__main__":
    main()
