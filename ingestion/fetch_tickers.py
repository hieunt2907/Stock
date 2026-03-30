import time
from config import get_vn30_tickers, produce_message, flush_producer

def fetch_vn_tickers_to_kafka():
    print("Đang lấy danh sách tickers VN30 từ nguồn KBS...")
    try:
        vn30_symbols = get_vn30_tickers()
        
        for idx, symbol in enumerate(vn30_symbols, 1):
            payload = {
                'ticker': symbol,
                'group': 'VN30',
                'type': 'ticker_list',
            }
            # Gửi message lên Kafka topic ticker_list
            produce_message(key=symbol, payload=payload, topic='ticker_list')
            
            if idx % 50 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
            else:
                time.sleep(1)
            
        flush_producer()
        print(f"Đã đẩy {len(vn30_symbols)} tickers VN30 lên Kafka.")
        return vn30_symbols
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu tickers: {e}")

if __name__ == "__main__":
    fetch_vn_tickers_to_kafka()


