import os
import json
import logging
from dotenv import load_dotenv
from confluent_kafka import Producer
from vnstock import Listing

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lấy các biến môi trường một cách an toàn
load_dotenv()

# Khởi tạo API Key từ môi trường (không ghi thông tin nhạy cảm vào code)
vnstock_api_key = os.getenv('VNSTOCK_API_KEY')
if vnstock_api_key and str(vnstock_api_key).strip():
    try:
        from vnstock import change_api_key
        change_api_key(vnstock_api_key)
        logger.info("Đã cấu hình API key thành công.")
    except Exception as e:
        logger.error(f"Lỗi khi cài đặt API Key: {e}")

# Cấu hình Kafka Kafka Producer
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_ADDRESS')
KAFKA_TOPIC = os.getenv('KAFKA_PRODUCER_TOPIC')

try:
    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
except Exception as e:
    print(f"Lỗi khởi tạo Kafka Producer: {e}")
    kafka_producer = None

def delivery_report(err, msg):
    """Callback được gọi khi message được gửi thành công hoặc thất bại"""
    if err is not None:
        print(f"Kafka delivery failed: {err}")

def get_vn30_tickers():
    """Hàm tiện ích lấy danh sách mã VN30"""
    return list(Listing(source='KBS').symbols_by_group('VN30'))

def produce_message(key, payload, topic=None):
    """Chuyển đổi Dict thành chuỗi JSON bytes và đẩy lên hệ thống Kafka"""
    if not kafka_producer:
        return
    
    # Use the provided topic or fall back to the default KAFKA_TOPIC
    target_topic = topic if topic else KAFKA_TOPIC
    
    message = json.dumps(payload, default=str).encode('utf-8')
    key_bytes = key.encode('utf-8') if key else None
    
    kafka_producer.produce(target_topic, key=key_bytes, value=message, callback=delivery_report)
    kafka_producer.poll(0)

def flush_producer():
    """Đảm bảo tất cả các message được đẩy đi trước khi kết thúc module"""
    if kafka_producer:
        kafka_producer.flush()
