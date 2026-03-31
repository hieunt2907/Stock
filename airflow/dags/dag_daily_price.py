from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import docker
import logging

# Cấu hình log để dễ debug trong Airflow UI
logger = logging.getLogger(__name__)

def run_command_in_container(container_name, command, workdir=None):
    """
    Hàm helper để thực thi lệnh bên trong một Docker container đang chạy.
    """
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        
        logger.info(f"Đang thực thi lệnh trên container {container_name}: {command}")
        
        # Thực thi lệnh (tương đương docker exec)
        exit_code, output = container.exec_run(
            cmd=command,
            workdir=workdir,
            environment={"PYTHONPATH": "/app/ingestion"}
        )
        
        result = output.decode().strip()
        print(result)
        
        if exit_code != 0:
            raise Exception(f"Lệnh thất bại với exit code {exit_code}. Chi tiết: {result}")
            
        return result
    except Exception as e:
        logger.error(f"Lỗi khi kết nối Docker: {str(e)}")
        raise

default_args = {
    'owner': 'hieunt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_daily_price_optimized',
    default_args=default_args,
    description='Pipeline fetch và xử lý giá chứng khoán hàng ngày',
    schedule_interval='0 17 * * 1-5',  # 17:00 từ Thứ 2 đến Thứ 6
    catchup=False,
    tags=['stock', 'ingestion', 'spark', 'shino'],
) as dag:

    # Task 1: Ingest dữ liệu từ API ném vào Kafka/MinIO
    ingest_task = PythonOperator(
        task_id='ingest_daily_prices_to_kafka',
        python_callable=run_command_in_container,
        op_kwargs={
            'container_name': 'daily-price-pipeline',
            'workdir': '/app/ingestion',
            'command': 'python fetch_daily_price.py'
        }
    )

    # Task 2: Spark Job đọc từ MinIO đẩy vào Postgres
    # Truyền partition_path (yyyy-MM-dd_HH) làm đối số cuối cùng cho spark-submit
    process_task = PythonOperator(
        task_id='process_daily_prices_to_postgres',
        python_callable=run_command_in_container,
        op_kwargs={
            'container_name': 'daily-price-pipeline',
            'command': (
                'spark-submit '
                '--class hieunt.stock.spark.job.DailyPriceJob '
                '/app/stock-app.jar '
                '{{ data_interval_end.strftime("%Y-%m-%d_%H") }}'
            )
        }
    )

    ingest_task >> process_task