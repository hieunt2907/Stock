import logging
import sys
import os

# Thêm thư mục hiện tại vào sys.path để có thể import các file cùng thư mục
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fetch_tickers import fetch_vn_tickers_to_kafka
from fetch_company_metadata import fetch_metadata
from fetch_index import fetch_benchmark_index
from backfill_historical_price import backfill_history
from fetch_daily_price import fetch_daily_incremental
from fetch_intraday_price import fetch_intraday

# Cấu hình logging cơ bản
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Data Ingestion Pipeline...")

    try:
        # 1. Lấy danh sách tickers VN30
        logger.info("Step 1: Fetching VN30 Tickers...")
        fetch_vn_tickers_to_kafka()
        
        # 2. Lấy thông tin metadata của công ty
        logger.info("Step 2: Fetching Company Metadata...")
        fetch_metadata()
        
        # 3. Lấy chỉ số benchmark VN30
        logger.info("Step 3: Fetching VN30 Benchmark Index...")
        fetch_benchmark_index()
        
        # 4. Backfill dữ liệu lịch sử
        logger.info("Step 4: Backfilling Historical Prices...")
        backfill_history()
        
        # 5. Lấy dữ liệu giá hàng ngày
        logger.info("Step 5: Fetching Daily Incremental Prices...")
        fetch_daily_incremental()
        
        # # 6. Lấy dữ liệu intraday
        # logger.info("Step 6: Fetching Intraday Prices...")
        # fetch_intraday()
        
        logger.info("Data Ingestion Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed at some step: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
