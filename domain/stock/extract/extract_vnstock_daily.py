from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from infrastructure.alpha_vantage_api_config import alpha_vantage_api_config, alpha_vantage_rate_limiter

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

OUTPUT_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "raw" / "alpha_vantage_daily_raw.json"
RESPONSE_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "raw" / "alpha_vantage_daily_response.json"


def _default_start_date(days_back: int = 2) -> str:
    return (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")


def _default_end_date() -> str:
    return date.today().strftime("%Y-%m-%d")


def _normalize_daily_frame(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    normalized = df.copy()
    if normalized.index.name and str(normalized.index.name).lower() in {"time", "date", "trading_date"}:
        normalized = normalized.reset_index()

    normalized.columns = [str(column).strip().lower() for column in normalized.columns]

    if "time" in normalized.columns:
        normalized = normalized.rename(columns={"time": "trading_date"})
    elif "date" in normalized.columns:
        normalized = normalized.rename(columns={"date": "trading_date"})
    elif "tradingdate" in normalized.columns:
        normalized = normalized.rename(columns={"tradingdate": "trading_date"})

    normalized["symbol"] = symbol

    if "value" not in normalized.columns:
        normalized["value"] = pd.NA

    missing_columns = [column for column in OUTPUT_COLUMNS if column not in normalized.columns]
    if missing_columns:
        raise ValueError(f"{symbol}: missing columns from daily price response: {missing_columns}")

    normalized["trading_date"] = pd.to_datetime(normalized["trading_date"]).dt.strftime("%Y-%m-%d")

    numeric_columns = ["open", "high", "low", "close", "volume", "value"]
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    if "volume" in normalized.columns:
        normalized["volume"] = normalized["volume"].astype("Int64")

    return normalized[OUTPUT_COLUMNS].sort_values(["symbol", "trading_date"]).reset_index(drop=True)


def load_schema(schema_path: Path) -> dict:
    with schema_path.open("r", encoding="utf-8") as schema_file:
        return json.load(schema_file)


def validate_daily_input_schema(df: pd.DataFrame, schema: dict | None = None) -> None:
    schema = schema or load_schema(OUTPUT_SCHEMA_PATH)
    required_columns = schema.get("required", [])
    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(f"Daily input schema missing columns: {missing_columns}")

    expected_columns = list(schema.get("properties", {}).keys())
    if expected_columns and list(df.columns) != expected_columns:
        raise ValueError(
            "Daily input schema columns do not match output columns: "
            f"expected {expected_columns}, got {list(df.columns)}"
        )


def validate_alpha_vantage_response_shape(payload: dict, schema: dict | None = None) -> None:
    schema = schema or load_schema(RESPONSE_SCHEMA_PATH)
    required_keys = schema.get("required", [])
    missing_keys = [key for key in required_keys if key not in payload]
    if missing_keys:
        raise ValueError(f"Alpha Vantage response missing keys: {missing_keys}")

    time_series = payload.get("Time Series (Daily)")
    if not isinstance(time_series, dict):
        raise ValueError("Alpha Vantage response key 'Time Series (Daily)' must be an object")

    required_price_keys = schema["properties"]["Time Series (Daily)"]["additionalProperties"]["required"]
    for trading_date, values in time_series.items():
        if not isinstance(values, dict):
            raise ValueError(f"Alpha Vantage response values for {trading_date} must be an object")

        missing_price_keys = [key for key in required_price_keys if key not in values]
        if missing_price_keys:
            raise ValueError(f"Alpha Vantage response for {trading_date} missing keys: {missing_price_keys}")


def _alpha_vantage_output_size(start_date: str, end_date: str) -> str:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return "full" if (end - start).days > 100 else "compact"


def _alpha_vantage_daily_payload_to_frame(payload: dict, symbol: str) -> pd.DataFrame:
    if "Error Message" in payload:
        raise ValueError(payload["Error Message"])

    if "Note" in payload:
        raise RuntimeError(payload["Note"])

    if "Information" in payload:
        raise RuntimeError(payload["Information"])

    validate_alpha_vantage_response_shape(payload)

    time_series = payload.get("Time Series (Daily)")
    if not isinstance(time_series, dict):
        raise ValueError("Alpha Vantage response missing 'Time Series (Daily)'")

    rows = []
    for trading_date, values in time_series.items():
        close = values.get("4. close")
        volume = values.get("5. volume")
        rows.append(
            {
                "symbol": symbol,
                "trading_date": trading_date,
                "open": values.get("1. open"),
                "high": values.get("2. high"),
                "low": values.get("3. low"),
                "close": close,
                "volume": volume,
                "value": float(close) * int(volume) if close is not None and volume is not None else pd.NA,
            }
        )

    return pd.DataFrame(rows)


def fetch_alpha_vantage_daily(
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    start = start_date or _default_start_date()
    end = end_date or _default_end_date()

    if not alpha_vantage_api_config.api_key:
        raise ValueError("Missing ALPHA_VANTAGE_API_KEY in environment or .env")

    alpha_vantage_rate_limiter.wait()
    try:
        response = requests.get(
            alpha_vantage_api_config.base_url,
            params={
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": _alpha_vantage_output_size(start, end),
                "apikey": alpha_vantage_api_config.api_key,
            },
            timeout=30,
        )
        response.raise_for_status()
        df_history = _alpha_vantage_daily_payload_to_frame(response.json(), symbol)
    finally:
        alpha_vantage_rate_limiter.mark_request()

    df_daily = _normalize_daily_frame(df_history, symbol)
    df_daily = df_daily[
        (df_daily["trading_date"] >= start)
        & (df_daily["trading_date"] <= end)
    ].reset_index(drop=True)

    if not df_daily.empty:
        validate_daily_input_schema(df_daily)

    return df_daily


def fetch_many_alpha_vantage_daily(
    symbols: Iterable[str],
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    tickers = [symbol.strip().upper() for symbol in symbols if symbol.strip()]

    for ticker in tickers:
        try:
            df_daily = fetch_alpha_vantage_daily(
                symbol=ticker,
                start_date=start_date,
                end_date=end_date,
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract daily historical price data from Alpha Vantage.")
    parser.add_argument("--symbols", required=True, help="Comma-separated tickers, for example IBM,MSFT,AAPL.")
    parser.add_argument("--start-date", default=_default_start_date(), help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", default=_default_end_date(), help="End date in YYYY-MM-DD format.")
    parser.add_argument("--output", help="Optional CSV output path.")
    args = parser.parse_args()

    df_daily = fetch_many_alpha_vantage_daily(
        symbols=_parse_symbols(args.symbols),
        start_date=args.start_date,
        end_date=args.end_date,
    )

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_daily.to_csv(output_path, index=False)
        print(f"Saved {len(df_daily)} rows to {output_path}")
    else:
        print(df_daily.to_csv(index=False))


if __name__ == "__main__":
    main()
