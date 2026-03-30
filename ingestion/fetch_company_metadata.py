import time
from vnstock import Company
from config import get_vn30_tickers, produce_message, flush_producer

def fetch_metadata():
    print("1. Đang lấy danh sách các công ty niêm yết (VN30)...")
    tickers = get_vn30_tickers()
    
    print(f"Tiến hành lấy overview cho {len(tickers)} công ty trong VN30...")
    for idx, ticker in enumerate(tickers, 1):
        try:
            df_overview = Company(symbol=ticker, source='KBS').overview()
            
            if not df_overview.empty:
                records = df_overview.to_dict(orient='records')
                for row in records:
                    row['ticker'] = ticker
                    row['type'] = 'company_metadata'
                    produce_message(key=ticker, payload=row, topic='company_metadata')
                
            print(f" -> Lấy overview cho {ticker} thành công.")
            if idx % 50 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
            else:
                time.sleep(1)
        except Exception as e:
            print(f" -> Lỗi với {ticker}: {e}")
            
    flush_producer()
    print("Hoàn tất gửi metadata lên Kafka.")

if __name__ == "__main__":
    fetch_metadata()


