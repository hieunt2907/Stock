import time
from datetime import datetime
from vnstock import Quote
from config import get_vn30_tickers, upload_dataframe_to_minio

TOPIC = 'daily_price_backfill'

def backfill_history():
    start_date = '2015-01-01'
    end_date   = datetime.now().strftime('%Y-%m-%d')
    tickers    = get_vn30_tickers()

    print(f"Bắt đầu backfill dữ liệu VN30 từ {start_date} đến {end_date} trực tiếp lên MinIO...")
    for idx, ticker in enumerate(tickers, 1):
        try:
            quote   = Quote(symbol=ticker, source='VCI')
            df_hist = quote.history(start=start_date, end=end_date, interval='1d')

            if not df_hist.empty:
                df_hist['ticker'] = ticker
                df_hist['type']   = TOPIC
                ok = upload_dataframe_to_minio(df_hist, topic=TOPIC, partition_key=ticker)
                if ok:
                    print(f" -> [{ticker}] Upload thành công {len(df_hist)} bản ghi lên MinIO.")
            else:
                print(f" -> [{ticker}] Không lấy được dữ liệu.")

            if idx % 50 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
        except Exception as e:
            print(f" -> [{ticker}] Lỗi: {e}")

    print("Hoàn thành backfill VN30 vào MinIO.")

if __name__ == "__main__":
    backfill_history()
