from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hieunt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'stock_daily_backfill_pipeline',
    default_args=default_args,
    description='Historical data backfill pipeline - Manual trigger only',
    schedule_interval=None,
    catchup=False,
    tags=['stock', 'ingestion', 'backfill'],
)

# Task 1: Ingest backfill historical price to Kafka
ingest_task = BashOperator(
    task_id='ingest_backfill_to_kafka',
    bash_command='docker exec daily-backfill-pipeline bash -c "cd /app/ingestion && PYTHONPATH=/app/ingestion python backfill_historical_price.py"',
    dag=dag,
)

# Task 2: Process historical from MinIO to Postgres using Spark
process_task = BashOperator(
    task_id='process_backfill_to_postgres',
    bash_command=(
        'docker exec daily-backfill-pipeline spark-submit --class hieunt.stock.spark.job.DailyPriceBackfillJob '
        '/app/stock-app.jar '
        '{{ execution_date.strftime("%Y-%m-%d_%H") }}'
    ),
    dag=dag,
)

ingest_task >> process_task
