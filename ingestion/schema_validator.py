"""
schema_validator.py
-------------------
Module validate schema DataFrame tại bước Extract trong ingestion pipeline.

Mỗi TOPIC có một SchemaDefinition riêng gồm:
    - required_columns : các cột BẮT BUỘC phải có trong DataFrame
    - column_types     : kỳ vọng dtype pandas cho từng cột (kiểm tra ở mức kind)
    - not_null_columns : các cột KHÔNG ĐƯỢC phép có giá trị null / NaN
    - numeric_ranges   : dict {column: (min, max)} — giá trị phải nằm trong khoảng

Hàm chính:
    validate_dataframe(df, topic) -> (is_valid: bool, report: dict)

Nếu validation PASS hoàn toàn → is_valid = True, report = {}
Nếu có lỗi                   → is_valid = False, report chứa chi tiết lỗi
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema Definitions
# ---------------------------------------------------------------------------

@dataclass
class SchemaDefinition:
    """Định nghĩa schema cho một topic ingestion."""
    required_columns: List[str] = field(default_factory=list)
    # pandas dtype 'kind': 'f'=float, 'i'=int, 'O'=object/string, 'M'=datetime, 'b'=bool
    column_types: Dict[str, str] = field(default_factory=dict)
    not_null_columns: List[str] = field(default_factory=list)
    numeric_ranges: Dict[str, Tuple[Optional[float], Optional[float]]] = field(default_factory=dict)


SCHEMAS: Dict[str, SchemaDefinition] = {

    # ------------------------------------------------------------------
    # daily_price: OHLCV từ Quote.history(interval='1d')
    # ------------------------------------------------------------------
    "daily_price": SchemaDefinition(
        required_columns=["time", "open", "high", "low", "close", "volume", "ticker"],
        column_types={
            "open":   "f",   # float
            "high":   "f",
            "low":    "f",
            "close":  "f",
            "volume": "fi",  # float hoặc int đều OK
            "ticker": "O",   # string/object
        },
        not_null_columns=["time", "open", "high", "low", "close", "volume", "ticker"],
        numeric_ranges={
            "open":   (0, None),
            "high":   (0, None),
            "low":    (0, None),
            "close":  (0, None),
            "volume": (0, None),
        },
    ),

    # ------------------------------------------------------------------
    # intraday_price: OHLCV từ Quote.history(interval='5m')
    # ------------------------------------------------------------------
    "intraday_price": SchemaDefinition(
        required_columns=["time", "open", "high", "low", "close", "volume", "ticker"],
        column_types={
            "open":   "f",
            "high":   "f",
            "low":    "f",
            "close":  "f",
            "volume": "fi",
            "ticker": "O",
        },
        not_null_columns=["time", "open", "high", "low", "close", "ticker"],
        numeric_ranges={
            "open":   (0, None),
            "high":   (0, None),
            "low":    (0, None),
            "close":  (0, None),
            "volume": (0, None),
        },
    ),

    # ------------------------------------------------------------------
    # benchmark_index: chỉ số VN30 từ Quote.history
    # ------------------------------------------------------------------
    "benchmark_index": SchemaDefinition(
        required_columns=["time", "open", "high", "low", "close", "volume", "ticker"],
        column_types={
            "open":   "f",
            "high":   "f",
            "low":    "f",
            "close":  "f",
            "volume": "fi",
            "ticker": "O",
        },
        not_null_columns=["time", "open", "high", "low", "close", "ticker"],
        numeric_ranges={
            "open":   (0, None),
            "high":   (0, None),
            "low":    (0, None),
            "close":  (0, None),
        },
    ),

    # ------------------------------------------------------------------
    # ticker_list: danh sách mã chứng khoán VN30
    # ------------------------------------------------------------------
    "ticker_list": SchemaDefinition(
        required_columns=["ticker", "group", "type"],
        column_types={
            "ticker": "O",
            "group":  "O",
            "type":   "O",
        },
        not_null_columns=["ticker", "group"],
        numeric_ranges={},
    ),

    # ------------------------------------------------------------------
    # company_metadata: tổng quan công ty từ Company.overview()
    # ------------------------------------------------------------------
    "company_metadata": SchemaDefinition(
        required_columns=["ticker"],
        column_types={
            "ticker": "O",
        },
        not_null_columns=["ticker"],
        numeric_ranges={},
    ),

    # ------------------------------------------------------------------
    # financial_ratio: chỉ số tài chính từ Finance().financial_ratio()
    # Áp dụng cho cả financial_ratio() và company_fundamental().
    # Business key trong ClickHouse: (ticker, year, quarter)
    # ------------------------------------------------------------------
    "financial_ratio": SchemaDefinition(
        required_columns=["ticker", "year", "quarter"],
        column_types={
            "ticker":  "O",   # string
            "year":    "fi",  # int hoặc float (pandas đọc từ JSON có thể là float64)
            "quarter": "O",   # string: Q1, Q2, Q3, Q4, Yearly
            # Các chỉ số tài chính — chỉ check khi tồn tại trong DataFrame
            "pe":                  "f",
            "pb":                  "f",
            "ps":                  "f",
            "ev_ebitda":           "f",
            "roe":                 "f",
            "roa":                 "f",
            "eps":                 "f",
            "net_profit_margin":   "f",
            "gross_profit_margin": "f",
            "asset_turnover":      "f",
            "current_ratio":       "f",
            "debt_to_equity":      "f",
        },
        not_null_columns=["ticker", "year", "quarter"],
        numeric_ranges={
            # Cho phép giá trị tự do (không cắt cụt), chỉ khai báo để kiểm tra sự tồn tại
            "pe": (None, None),
            "pb": (None, None),
        },
    ),
}


# ---------------------------------------------------------------------------
# Validation Logic
# ---------------------------------------------------------------------------

def _check_required_columns(df: pd.DataFrame, schema: SchemaDefinition) -> List[str]:
    """Kiểm tra các cột bắt buộc có trong DataFrame không."""
    missing = [col for col in schema.required_columns if col not in df.columns]
    return missing


def _check_column_types(df: pd.DataFrame, schema: SchemaDefinition) -> Dict[str, str]:
    """
    Kiểm tra dtype pandas của các cột theo kỳ vọng.
    column_types hỗ trợ multi-kind: 'fi' = float hoặc int đều OK.
    """
    errors: Dict[str, str] = {}
    for col, expected_kinds in schema.column_types.items():
        if col not in df.columns:
            continue  # đã bắt ở required_columns
        actual_kind = df[col].dtype.kind
        if actual_kind not in expected_kinds:
            errors[col] = (
                f"dtype không khớp — kỳ vọng kind in '{expected_kinds}', "
                f"thực tế '{actual_kind}' ({df[col].dtype})"
            )
    return errors


def _check_not_null(df: pd.DataFrame, schema: SchemaDefinition) -> Dict[str, int]:
    """Đếm số giá trị null trong các cột không được phép null."""
    errors: Dict[str, int] = {}
    for col in schema.not_null_columns:
        if col not in df.columns:
            continue
        null_count = int(df[col].isnull().sum())
        if null_count > 0:
            errors[col] = null_count
    return errors


def _check_numeric_ranges(
    df: pd.DataFrame, schema: SchemaDefinition
) -> Dict[str, dict]:
    """Kiểm tra giá trị nằm ngoài khoảng [min_val, max_val] nếu được định nghĩa."""
    errors: Dict[str, dict] = {}
    for col, (min_val, max_val) in schema.numeric_ranges.items():
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors='coerce')
        violations = {}
        if min_val is not None:
            below = int((series < min_val).sum())
            if below:
                violations["below_min"] = {"min": min_val, "count": below}
        if max_val is not None:
            above = int((series > max_val).sum())
            if above:
                violations["above_max"] = {"max": max_val, "count": above}
        if violations:
            errors[col] = violations
    return errors


def validate_dataframe(
    df: pd.DataFrame,
    topic: str,
    ticker: Optional[str] = None,
) -> Tuple[bool, dict]:
    """
    Validate DataFrame theo schema của topic.

    Args:
        df     : DataFrame cần validate
        topic  : tên topic (phải có trong SCHEMAS)
        ticker : (tuỳ chọn) mã cổ phiếu, chỉ dùng cho log

    Returns:
        (is_valid, report)
        - is_valid: True nếu KHÔNG có lỗi nào
        - report  : dict chứa chi tiết lỗi (rỗng nếu pass)
    """
    tag = f"[{topic}]" + (f"[{ticker}]" if ticker else "")

    if topic not in SCHEMAS:
        logger.warning(f"{tag} Không tìm thấy schema cho topic '{topic}', bỏ qua validation.")
        return True, {}

    schema = SCHEMAS[topic]
    report: dict = {}

    # 1. Cột bắt buộc
    missing_cols = _check_required_columns(df, schema)
    if missing_cols:
        report["missing_columns"] = missing_cols

    # 2. Kiểu dữ liệu
    type_errors = _check_column_types(df, schema)
    if type_errors:
        report["type_errors"] = type_errors

    # 3. Not-null
    null_errors = _check_not_null(df, schema)
    if null_errors:
        report["null_violations"] = null_errors

    # 4. Numeric range
    range_errors = _check_numeric_ranges(df, schema)
    if range_errors:
        report["range_violations"] = range_errors

    is_valid = len(report) == 0

    if is_valid:
        logger.info(
            f"{tag} Schema validation PASSED — "
            f"{len(df)} dòng, {len(df.columns)} cột."
        )
    else:
        logger.warning(
            f"{tag} Schema validation FAILED — "
            f"{len(df)} dòng. Chi tiết lỗi: {report}"
        )

    return is_valid, report
