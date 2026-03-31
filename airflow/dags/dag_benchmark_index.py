from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hieunt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_benchmark_index_pipeline',
    default_args=default_args,
    description='Pipeline to fetch VNINDEX and other indices',
    schedule_interval='@daily',
    catchup=False,
    tags=['stock', 'ingestion', 'index'],
)

# Task 1: Ingest VNINDEX from API to Kafka
ingest_task = BashOperator(
    task_id='ingest_indices_to_kafka',
    bash_command='docker exec benchmark-index-pipeline bash -c "cd /app/ingestion && PYTHONPATH=/app/ingestion python fetch_index.py"',
    dag=dag,
)

# Task 2: Process Indices from MinIO to Postgres using Spark
process_task = BashOperator(
    task_id='process_indices_to_postgres',
    bash_command=(
        'docker exec benchmark-index-pipeline spark-submit --class hieunt.stock.spark.job.BenchmarkIndexJob '
        '/app/stock-app.jar '
        '{{ execution_date.strftime("%Y-%m-%d_%H") }}'
    ),
    dag=dag,
)

ingest_task >> process_task
