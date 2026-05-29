from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from infrastructure.clickhouse_config import get_clickhouse_client, optimize_table_final


DATA_MART_SQL_PATH = PROJECT_ROOT / "domain" / "stock" / "sql" / "data_mart.sql"
MART_TABLES = [
    "mart_market_summary",
    "mart_top_gainers",
    "mart_top_liquidity",
    "mart_sector_performance",
]


def _split_sql_statements(sql_text: str) -> list[str]:
    statements = []
    current_statement = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue

        current_statement.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current_statement).strip().rstrip(";")
            if statement:
                statements.append(statement)
            current_statement = []

    if current_statement:
        statements.append("\n".join(current_statement).strip().rstrip(";"))

    return statements


def _prepare_statement(statement: str) -> str:
    create_match = re.match(r"CREATE\s+TABLE\s+(\w+)", statement, flags=re.IGNORECASE)
    if create_match:
        table_name = create_match.group(1)
        return re.sub(
            rf"CREATE\s+TABLE\s+{table_name}",
            f"CREATE TABLE IF NOT EXISTS {table_name}",
            statement,
            count=1,
            flags=re.IGNORECASE,
        )

    return statement


def build_data_marts(sql_path: Path = DATA_MART_SQL_PATH, optimize_final: bool = True) -> None:
    client = get_clickhouse_client()
    statements = [_prepare_statement(statement) for statement in _split_sql_statements(sql_path.read_text(encoding="utf-8"))]

    for table_name in MART_TABLES:
        client.command(f"TRUNCATE TABLE IF EXISTS {table_name}")

    for statement in statements:
        client.command(statement)

    if optimize_final:
        for table_name in MART_TABLES:
            optimize_table_final(table_name=table_name)

    print(f"Built data marts: {', '.join(MART_TABLES)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ClickHouse data mart tables from fact and dimension tables.")
    parser.add_argument("--sql-path", default=str(DATA_MART_SQL_PATH), help="Path to data mart SQL file.")
    parser.add_argument("--skip-optimize", action="store_true", help="Skip OPTIMIZE TABLE ... FINAL after inserts.")
    args = parser.parse_args()

    build_data_marts(sql_path=Path(args.sql_path), optimize_final=not args.skip_optimize)


if __name__ == "__main__":
    main()
