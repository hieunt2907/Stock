import time
import pandas as pd
from vnstock import Company
from config import get_vn30_tickers, upload_dataframe_to_minio

TOPIC = 'company_metadata'

def fetch_metadata():
    print("1. Đang lấy danh sách các công ty niêm yết (VN30)...")
    tickers = get_vn30_tickers()

    print(f"Tiến hành lấy overview cho {len(tickers)} công ty trong VN30...")
    for idx, ticker in enumerate(tickers, 1):
        try:
            df_overview = Company(symbol=ticker, source='KBS').overview()

            if not df_overview.empty:
                df_overview['ticker'] = ticker
                df_overview['type']   = TOPIC
                ok = upload_dataframe_to_minio(df_overview, topic=TOPIC, partition_key=ticker)
                if ok:
                    print(f" -> [{ticker}] Upload overview thành công lên MinIO.")
            else:
                print(f" -> [{ticker}] Không có dữ liệu overview.")

            if idx % 50 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
            else:
                time.sleep(1)
        except Exception as e:
            print(f" -> Lỗi với {ticker}: {e}")

    print("Hoàn tất gửi metadata lên MinIO.")

if __name__ == "__main__":
    fetch_metadata()
