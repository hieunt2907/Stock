import os
import io
import json
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from vnstock import Listing

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load .env từ thư mục cha (root project) — hoạt động cả khi chạy local lẫn trong Docker
# Docker: /app/ingestion/config.py -> tìm /app/.env (được mount từ host)
# Local:  e:/Data Engineer/Stock/ingestion/config.py -> tìm e:/Data Engineer/Stock/.env
_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_base_dir, '.env'))

# Khởi tạo API Key từ môi trường (không ghi thông tin nhạy cảm vào code)
vnstock_api_key = os.getenv('VNSTOCK_API_KEY')
if vnstock_api_key and str(vnstock_api_key).strip():
    try:
        from vnstock import change_api_key
        change_api_key(vnstock_api_key)
        logger.info("Đã cấu hình API key thành công.")
    except Exception as e:
        logger.error(f"Lỗi khi cài đặt API Key: {e}")

# Cấu hình MinIO
# MinIO SDK chỉ nhận 'host:port', không nhận 'http://host:port'
# → tự động strip scheme nếu có (http:// hoặc https://)
_raw_endpoint   = os.getenv('MINIO_ENDPOINT')
MINIO_ENDPOINT  = _raw_endpoint.split('://')[-1].rstrip('/')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET    = os.getenv('MINIO_BUCKET')
MINIO_SECURE    = os.getenv('MINIO_SECURE', 'false').lower() == 'true'

# Khởi tạo MinIO client
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    # Tạo bucket nếu chưa tồn tại
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        logger.info(f"Đã tạo bucket '{MINIO_BUCKET}'.")
    logger.info(f"Kết nối MinIO thành công: {MINIO_ENDPOINT} (secure={MINIO_SECURE})")
except Exception as e:
    logger.error(f"Lỗi khởi tạo MinIO client: {e}")
    minio_client = None


def get_vn30_tickers() -> list:
    """Hàm tiện ích lấy danh sách mã VN30"""
    return list(Listing(source='KBS').symbols_by_group('VN30'))


def upload_dataframe_to_minio(df: pd.DataFrame, topic: str, partition_key: str = None) -> bool:
    """
    Upload một DataFrame lên MinIO dưới dạng Parquet.

    Object path:
        <topic>/<YYYY-MM-DD_HH>/<partition_key>.parquet   (nếu có partition_key)
        <topic>/<YYYY-MM-DD_HH>/data.parquet               (nếu không có partition_key)

    Returns:
        True nếu upload thành công, False nếu thất bại.
    """
    if minio_client is None:
        logger.error("MinIO client chưa được khởi tạo, bỏ qua upload.")
        return False

    if df is None or df.empty:
        logger.warning(f"[{topic}] DataFrame rỗng, bỏ qua.")
        return False

    try:
        hour_partition = datetime.now().strftime('%Y-%m-%d_%H')
        filename = f"{partition_key}.json" if partition_key else "data.json"
        object_name = f"{topic}/{hour_partition}/{filename}"

        # Chuyển DataFrame sang JSON bytes (in-memory, không ghi file)
        json_str = df.to_json(orient='records', date_format='iso', force_ascii=False)
        json_bytes = json_str.encode('utf-8')
        buffer = io.BytesIO(json_bytes)
        size = len(json_bytes)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=buffer,
            length=size,
            content_type='application/json',
        )
        logger.info(f"[{topic}] Upload thành công: s3a://{MINIO_BUCKET}/{object_name} ({size:,} bytes)")
        return True
    except S3Error as e:
        logger.error(f"[{topic}] Lỗi MinIO S3: {e}")
        return False
    except Exception as e:
        logger.error(f"[{topic}] Lỗi không xác định khi upload: {e}")
        return False
