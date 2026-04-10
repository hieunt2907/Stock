import time
import pandas as pd
from datetime import datetime, timedelta
from vnstock import Quote
from config import upload_dataframe_to_minio

TOPIC = 'benchmark_index'

def fetch_benchmark_index():
    end_date   = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

    print(f"Đang lấy chỉ số benchmark VN30 từ {start_date} đến {end_date}...")

    try:
        quote    = Quote(symbol='VN30', source='VCI')
        df_index = quote.history(start=start_date, end=end_date, interval='1d')

        if not df_index.empty:
            df_index['ticker'] = 'VN30_INDEX'
            df_index['type']   = TOPIC
            ok = upload_dataframe_to_minio(df_index, topic=TOPIC, partition_key='VN30_INDEX')
            if ok:
                print(f"Gửi thành công chỉ số VN30: {len(df_index)} dòng lên MinIO.")
        else:
            print("Không có dữ liệu trả về cho VN30.")
    except Exception as e:
        print(f"Lỗi khi lấy chỉ số VN30: {e}")

if __name__ == "__main__":
    fetch_benchmark_index()
