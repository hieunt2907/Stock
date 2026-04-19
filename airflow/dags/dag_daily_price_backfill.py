from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from orchestrators import PythonSubmitOrchestrator, SparkDockerExecOrchestrator

default_args = {
    'owner': 'hieunt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    'stock_daily_backfill_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['stock', 'ingestion', 'backfill', 'shino'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_backfill_to_kafka',
        python_callable=PythonSubmitOrchestrator,
        op_kwargs={'script_name': 'backfill_historical_price.py'},
    )

    process_task = PythonOperator(
        task_id='process_backfill_to_postgres',
        python_callable=SparkDockerExecOrchestrator,
        op_kwargs={
            'spark_class': 'hieunt.stock.spark.job.DailyPriceBackfillJob',
            'partition_path': None,
        },
    )

    ingest_task >> process_task
