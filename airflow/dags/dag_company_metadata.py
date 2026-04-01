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
    'stock_company_metadata_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['stock', 'ingestion', 'metadata', 'shino'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_metadata_to_kafka',
        python_callable=PythonSubmitOrchestrator,
        op_kwargs={'script_name': 'fetch_company_metadata.py'},
    )

    process_task = PythonOperator(
        task_id='process_metadata_to_postgres',
        python_callable=SparkDockerExecOrchestrator,
        op_kwargs={
            'spark_class': 'hieunt.stock.spark.job.CompanyMetadataJob',
            'partition_path': '{{ macros.datetime.now().strftime("%Y-%m-%d_%H") }}',
        },
    )

    ingest_task >> process_task
