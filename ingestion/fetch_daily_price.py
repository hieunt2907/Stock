import time
import pandas as pd
from datetime import datetime, timedelta
from vnstock import Quote
from config import get_vn30_tickers, upload_dataframe_to_minio

TOPIC = 'daily_price'

def fetch_daily_incremental():
    end_date   = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

    tickers = get_vn30_tickers()
    print(f"Đang chạy incremental load cho {len(tickers)} mã VN30 từ {start_date} đến {end_date}...")

    for idx, ticker in enumerate(tickers, 1):
        try:
            quote    = Quote(symbol=ticker, source='KBS')
            df_hist  = quote.history(start=start_date, end=end_date, interval='1d')

            if not df_hist.empty:
                df_hist['ticker'] = ticker
                df_hist['type']   = TOPIC
                ok = upload_dataframe_to_minio(df_hist, topic=TOPIC, partition_key=ticker)
                if ok:
                    print(f" -> [{ticker}] Upload thành công {len(df_hist)} bản ghi lên MinIO.")
            else:
                print(f" -> [{ticker}] Không có dữ liệu.")

            if idx % 50 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
            else:
                time.sleep(1)
        except Exception as e:
            print(f" -> [{ticker}] Lỗi: {e}")

    print("Hoàn tất đẩy dữ liệu daily lên MinIO.")

if __name__ == "__main__":
    fetch_daily_incremental()