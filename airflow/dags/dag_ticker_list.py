from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from orchestrators import PythonSubmitOrchestrator, SparkDockerExecOrchestrator

default_args = {
    'owner': 'hieunt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_ticker_list_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['stock', 'ingestion', 'ticker', 'shino'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_tickers_to_kafka',
        python_callable=PythonSubmitOrchestrator,
        op_kwargs={'script_name': 'fetch_tickers.py'},
    )

    process_task = PythonOperator(
        task_id='process_tickers_to_postgres',
        python_callable=SparkDockerExecOrchestrator,
        op_kwargs={
            'spark_class': 'hieunt.stock.spark.job.TickerListJob',
            'partition_path': '{{ macros.datetime.now().strftime("%Y-%m-%d_%H") }}',
        },
    )

    ingest_task >> process_task
