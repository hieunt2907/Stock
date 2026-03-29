import time
from datetime import datetime, timedelta
from vnstock import Quote
from config import get_vn30_tickers, produce_message, flush_producer

def fetch_daily_incremental():
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    tickers = get_vn30_tickers()
    
    print(f"Đang chạy incremental load cho {len(tickers)} mã VN30 từ {start_date} đến {end_date}...")
    for idx, ticker in enumerate(tickers, 1):
        try:
            quote = Quote(symbol=ticker, source='KBS') 
            df_hist = quote.history(start=start_date, end=end_date, interval='1d')
            
            if not df_hist.empty:
                records = df_hist.to_dict(orient='records')
                count = 0
                for row in records:
                    row['ticker'] = ticker
                    row['type'] = 'daily_price'
                    produce_message(key=ticker, payload=row)
                    count += 1
                
                print(f" -> [{ticker}] Đã gửi {count} bản ghi lên Kafka.")
            else:
                print(f" -> [{ticker}] Không có dữ liệu.")
            
            if idx % 25 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
        except Exception as e:
            print(f" -> [{ticker}] Lỗi: {e}")

    flush_producer()
    print("Hoàn tất đẩy dữ liệu cấu trúc daily lên Kafka.")


