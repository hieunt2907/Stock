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
    'stock_daily_price_pipeline',
    default_args=default_args,
    schedule_interval='0 17 * * 1-5',
    catchup=False,
    tags=['stock', 'spark', 'shino'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_daily_prices',
        python_callable=PythonSubmitOrchestrator,
        op_kwargs={'script_name': 'fetch_daily_price.py'},
    )

    process_task = PythonOperator(
        task_id='process_daily_prices_spark',
        python_callable=SparkDockerExecOrchestrator,
        op_kwargs={
            'spark_class': 'hieunt.stock.spark.job.DailyPriceJob',
            'partition_path': '{{ macros.datetime.now().strftime("%Y-%m-%d_%H") }}',
        },
    )

    ingest_task >> process_task