from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from orchestrators import PythonSubmitOrchestrator, SparkDockerExecOrchestrator

default_args = {
    'owner': 'hieunt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'stock_financial_ratio_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['stock', 'ingestion', 'financial_ratio', 'shino'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_financial_ratio_to_minio',
        python_callable=PythonSubmitOrchestrator,
        op_kwargs={'script_name': 'fetch_financial_ratio.py'},
        # Financial ratio fetch có sleep rates → cho phép chạy lâu hơn
        execution_timeout=timedelta(hours=2),
    )

    process_task = PythonOperator(
        task_id='process_financial_ratio_to_clickhouse',
        python_callable=SparkDockerExecOrchestrator,
        op_kwargs={
            'spark_class': 'hieunt.stock.spark.job.FinancialRatioJob',
            'partition_path': '{{ macros.datetime.now().strftime("%Y-%m-%d_%H") }}',
        },
    )

    ingest_task >> process_task
