from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"
PYTHON_BIN = "python"


def project_command(command: str) -> str:
    return (
        "set -euxo pipefail; "
        f"cd {PROJECT_DIR}; "
        "echo JAVA_HOME=${JAVA_HOME:-}; "
        "java -version; "
        f"{PYTHON_BIN} --version; "
        f"{command}"
    )


default_args = {
    "owner": "stock-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="yfinance_daily_extract_transform_load",
    description="Extract yfinance prices and company profiles, transform with Spark, and load to ClickHouse.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["stock", "yfinance", "spark", "clickhouse"],
) as dag:
    extract_yfinance_company = BashOperator(
        task_id="extract_yfinance_company",
        bash_command=project_command(
            f"{PYTHON_BIN} domain/stock/extract/extract_yfinance_company.py"
        ),
    )

    transform_yfinance_company = BashOperator(
        task_id="transform_yfinance_company",
        bash_command=project_command(
            f"{PYTHON_BIN} domain/stock/tranform/tranform_yfinance_company.py"
        ),
    )

    load_yfinance_company = BashOperator(
        task_id="load_yfinance_company",
        bash_command=project_command(
            f"{PYTHON_BIN} domain/stock/load/load_yfinance_company.py"
        ),
    )

    extract_yfinance_daily = BashOperator(
        task_id="extract_yfinance_daily",
        bash_command=project_command(
            f"{PYTHON_BIN} domain/stock/extract/extract_yfinance_daily.py "
            "--start-date 2021-01-01 "
            "--end-date now"
        ),
    )

    transform_yfinance_daily = BashOperator(
        task_id="transform_yfinance_daily",
        bash_command=project_command(
            f"{PYTHON_BIN} domain/stock/tranform/tranform_yfinace_daily.py"
        ),
    )

    load_yfinance_daily = BashOperator(
        task_id="load_yfinance_daily",
        bash_command=project_command(
            f"{PYTHON_BIN} domain/stock/load/load_yfinance_daily.py"
        ),
    )

    build_data_marts = BashOperator(
        task_id="build_data_marts",
        bash_command=project_command(
            f"{PYTHON_BIN} domain/stock/load/build_data_marts.py"
        ),
    )

    extract_yfinance_company >> transform_yfinance_company >> load_yfinance_company
    extract_yfinance_daily >> transform_yfinance_daily
    [load_yfinance_company, transform_yfinance_daily] >> load_yfinance_daily
    load_yfinance_daily >> build_data_marts
