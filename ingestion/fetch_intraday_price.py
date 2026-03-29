import time
from vnstock import Quote
from config import get_vn30_tickers, produce_message, flush_producer

def fetch_intraday():
    tickers = get_vn30_tickers()
    
    print(f"Đang lấy dữ liệu intraday cho {len(tickers)} tickers VN30...")
    for idx, ticker in enumerate(tickers, 1):
        try:
            quote = Quote(symbol=ticker, source='VCI')
            df_intraday = quote.history(interval='5m')
            
            if not df_intraday.empty:
                records = df_intraday.to_dict(orient='records')
                count = 0
                for row in records:
                    row['ticker'] = ticker
                    row['type'] = 'intraday_price'
                    produce_message(key=ticker, payload=row)
                    count += 1
                
                print(f" -> [{ticker}] Gửi thành công {count} dòng intraday lên Kafka.")
            
            if idx % 25 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
            else:
                time.sleep(1)
        except Exception as e:
            print(f" -> [{ticker}] Lỗi: {e}")

    flush_producer()
    print("Hoàn tất gửi Intraday Data.")


