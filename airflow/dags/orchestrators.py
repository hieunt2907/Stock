"""
orchestrators.py — Shared helpers cho tất cả Stock DAGs.

PythonSubmitOrchestrator:
    Chạy Python ingestion script trực tiếp trong Airflow worker process.

SparkDockerExecOrchestrator:
    Chạy spark-submit bên trong spark-master container qua docker exec.
    - Không cần Spark binary trong Airflow worker
    - Không cần Livy, không cần config phức tạp
    - JAR mount vào spark-master qua docker-compose volume
    - .env mount vào spark-master và load thành environment vars
"""

import logging
import os
import subprocess
import sys

import docker

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
INGESTION_DIR     = "/app/ingestion"
SPARK_HOME        = "/opt/spark"
DOCKER_NETWORK    = "data-network"
JAR_PATH_IN_SPARK = "/app/spark-job.jar"
DOTENV_PATH       = "/app/.env"


# ─────────────────────────────────────────────────────────────
# Helper: parse .env
# ─────────────────────────────────────────────────────────────
def _load_dotenv(path: str = DOTENV_PATH) -> dict:
    """
    Parse file .env thành dict, bỏ qua comment và dòng trống.
    Không cần thư viện python-dotenv.
    """
    env = {}
    if not os.path.exists(path):
        logger.warning(f"[dotenv] Không tìm thấy {path}, bỏ qua.")
        return env

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            env[key.strip()] = val.strip().strip('"').strip("'")

    logger.info(f"[dotenv] Loaded {len(env)} biến từ {path}")
    return env


# ─────────────────────────────────────────────────────────────
# PythonSubmitOrchestrator
# ─────────────────────────────────────────────────────────────
def PythonSubmitOrchestrator(script_name: str, **kwargs):
    """Chạy Python ingestion script trực tiếp trong Airflow worker."""
    script_path = os.path.join(INGESTION_DIR, script_name)

    if not os.path.exists(script_path):
        raise FileNotFoundError(
            f"Script không tìm thấy tại {script_path}. "
            "Kiểm tra volume mount '../ingestion:/app/ingestion' trong docker-compose."
        )

    env = {**os.environ, "PYTHONPATH": INGESTION_DIR}
    logger.info(f"[Python] Chạy script: {script_path}")

    result = subprocess.run(
        [sys.executable, script_path],
        env=env,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info(f"[Python STDOUT]\n{result.stdout.strip()}")
    if result.stderr:
        logger.warning(f"[Python STDERR]\n{result.stderr.strip()}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Script '{script_name}' thất bại (exit {result.returncode}):\n{result.stderr}"
        )

    return result.stdout.strip()


# ─────────────────────────────────────────────────────────────
# SparkDockerExecOrchestrator
# ─────────────────────────────────────────────────────────────
def SparkDockerExecOrchestrator(
    spark_class: str,
    partition_path: str | None = None,
    spark_master_container: str = "spark-master",
    **kwargs,
):
    """
    Chạy spark-submit bên trong spark-master container qua docker exec.

    Args:
        spark_class:             Fully-qualified Spark main class,
                                 vd 'hieunt.stock.spark.job.DailyPriceJob'
        partition_path:          Argument truyền vào Spark job (có thể None)
        spark_master_container:  Tên container spark-master trong Docker
    """
    client    = docker.from_env()
    container = client.containers.get(spark_master_container)

    # Load .env để truyền vào exec_run
    env_vars = _load_dotenv(DOTENV_PATH)

    app_name = spark_class.split(".")[-1]  # vd: DailyPriceJob

    cmd = [
        f"{SPARK_HOME}/bin/spark-submit",
        "--master",      "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--name",        app_name,
        "--class",       spark_class,
        JAR_PATH_IN_SPARK,
    ]
    if partition_path:
        cmd.append(partition_path)

    logger.info(f"[DockerExec] Container : {spark_master_container}")
    logger.info(f"[DockerExec] Class     : {spark_class}")
    logger.info(f"[DockerExec] JAR       : {JAR_PATH_IN_SPARK}")
    logger.info(f"[DockerExec] Partition : {partition_path}")
    logger.info(f"[DockerExec] Command   : {' '.join(cmd)}")

    exit_code, output = container.exec_run(
        cmd,
        demux=False,
        stream=False,
        environment=env_vars,
    )

    decoded = output.decode(errors="replace") if isinstance(output, bytes) else (output or "")

    if decoded:
        logger.info(f"[DockerExec OUTPUT]\n{decoded.strip()}")

    if exit_code != 0:
        raise RuntimeError(
            f"spark-submit '{spark_class}' thất bại trong '{spark_master_container}' "
            f"(exit {exit_code}):\n{decoded}"
        )

    return decoded.strip()