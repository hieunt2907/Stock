import time
from datetime import datetime, timedelta
from vnstock import Quote
from config import produce_message, flush_producer

def fetch_benchmark_index():
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    print(f"Đang lấy chỉ số benchmark VN30 từ {start_date} đến {end_date}...")
    
    try:
        quote = Quote(symbol='VN30', source='VCI')
        df_index = quote.history(start=start_date, end=end_date, interval='1d')
        
        if not df_index.empty:
            records = df_index.to_dict(orient='records')
            count = 0
            for idx, row in enumerate(records, 1):
                row['ticker'] = 'VN30_INDEX'
                row['type'] = 'benchmark_index'
                produce_message(key='VN30_INDEX', payload=row, topic='benchmark_index')
                count += 1
                
                if idx % 50 == 0:
                    print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                    time.sleep(20)
                else:
                    time.sleep(1)
                
            flush_producer()
            print(f"Gửi thành công chỉ số VN30: {count} dòng lên Kafka.")
        else:
            print("Không có dữ liệu trả về cho VN30.")
    except Exception as e:
        print(f"Lỗi khi lấy chỉ số VN30: {e}")

if __name__ == "__main__":
    fetch_benchmark_index()



