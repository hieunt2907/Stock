from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import docker
import logging

# Cấu hình log để theo dõi trên Airflow UI
logger = logging.getLogger(__name__)

def run_command_in_container(container_name, command=None, **kwargs):
    """
    Hàm thực thi lệnh trong container. 
    Fix lỗi NoneType khi templates_dict tồn tại nhưng mang giá trị None.
    """
    # Lấy templates_dict từ kwargs, nếu nó là None thì gán thành dict trống {}
    templates_dict = kwargs.get('templates_dict') or {}
    rendered_command = templates_dict.get('rendered_command')
    
    # Ưu tiên lệnh đã render, nếu không có thì dùng lệnh tĩnh (command)
    final_command = rendered_command if rendered_command else command

    if not final_command:
        raise ValueError("Không tìm thấy lệnh (command) để thực thi!")

    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        
        logger.info(f"Container: {container_name}")
        logger.info(f"Đang thực thi: {final_command}")
        
        # Chạy lệnh exec
        exit_code, output = container.exec_run(
            cmd=final_command,
            environment={"PYTHONPATH": "/app/ingestion"}
        )
        
        result = output.decode().strip()
        print(result) 
        
        if exit_code != 0:
            raise Exception(f"Lệnh thất bại (Exit {exit_code}): {result}")
            
        return result
    except Exception as e:
        logger.error(f"Lỗi Docker: {str(e)}")
        raise

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

    # Task 1: Ingest Metadata from API to Kafka
    ingest_task = PythonOperator(
        task_id='ingest_metadata_to_kafka',
        python_callable=run_command_in_container,
        op_kwargs={
            'container_name': 'company-metadata-pipeline',
            'command': 'python /app/ingestion/fetch_company_metadata.py'
        }
    )

    # Task 2: Process Metadata from MinIO to Postgres using Spark
    process_task = PythonOperator(
        task_id='process_metadata_to_postgres',
        python_callable=run_command_in_container,
        op_kwargs={
            'container_name': 'company-metadata-pipeline'
        },
        templates_dict={
            'rendered_command': (
                'java -cp "/app/stock-app.jar:/opt/spark/jars/*" '
                'hieunt.stock.spark.job.CompanyMetadataJob '
                '{{ data_interval_end.strftime("%Y-%m-%d_%H") }}'
            )
        }
    )

    ingest_task >> process_task
