import time
import pandas as pd
from vnstock import Listing
from config import produce_message, flush_producer, logger

def fetch_all_listing():
    """
    Lấy danh sách tất cả các mã chứng khoánniêm yết thông qua Listing của vnstock.
    """
    logger.info("Đang lấy danh sách toàn bộ các công ty niêm yết từ vnstock Listing...")
    try:
        # Khởi tạo Listing với source KBS (hoặc source khác tùy nhu cầu)
        listing = Listing(source='KBS')
        
        # Lấy toàn bộ symbol
        df_listing = listing.all_symbols()
        
        if df_listing.empty:
            logger.warning("Không tìm thấy dữ liệu listing.")
            return

        total_count = len(df_listing)
        logger.info(f"Tìm thấy {total_count} mã chứng khoán.")

        # Duyệt qua từng dòng và gửi lên Kafka
        for idx, row in df_listing.iterrows():
            ticker = row.get('ticker')
            company_name = row.get('organ_name') # Tên công ty thường nằm ở cột organ_name hoặc tương đương
            
            payload = {
                'ticker': ticker,
                'company_name': company_name,
                'organ_short_name': row.get('organ_short_name'),
                'ticker_type': row.get('ticker_type'),
                'com_group_code': row.get('com_group_code'), # Sàn HOSE, HNX, UPCOM...
                'type': 'company_listing'
            }
            
            # Gửi message lên Kafka topic ticker_list
            produce_message(key=ticker, payload=payload, topic='ticker_list')
            
            # Rate Limit: Dừng 20s sau mỗi 50 requests (hoặc items)
            if (idx + 1) % 50 == 0:
                logger.info(f"[Rate Limit] Dừng 20s sau {idx + 1} items...")
                time.sleep(20)
            else:
                time.sleep(1)

            # Print tiến độ mỗi 100 mã
            if (idx + 1) % 100 == 0:
                logger.info(f"Đã xử lý {idx + 1}/{total_count} mã.")

        flush_producer()
        logger.info(f"Hoàn tất gửi {total_count} mã chứng khoán từ Listing lên Kafka.")
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu listing: {e}")

