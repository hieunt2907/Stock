import time
import pandas as pd
from vnstock import Finance
from config import get_vn30_tickers, upload_dataframe_to_minio
from schema_validator import validate_dataframe

TOPIC = 'financial_ratio'

def fetch_financial_ratio():
    tickers = get_vn30_tickers()
    print(f"Đang lấy financial ratios cho {len(tickers)} mã VN30...")

    for idx, ticker in enumerate(tickers, 1):
        try:
            df = Finance(symbol=ticker, source='VCI').financial_ratio()

            if not df.empty:
                df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
                df = df.reset_index()
                df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
                df['ticker'] = ticker
                df['type']   = TOPIC

                # --- Schema Validation ---
                is_valid, report = validate_dataframe(df, topic=TOPIC, ticker=ticker)
                if not is_valid:
                    print(f" -> [{ticker}] Bỏ qua upload do schema không hợp lệ: {report}")
                    continue

                ok = upload_dataframe_to_minio(df, topic=TOPIC, partition_key=ticker)
                if ok:
                    print(f" -> [{ticker}] Upload thành công {len(df)} bản ghi lên MinIO.")
            else:
                print(f" -> [{ticker}] Không có dữ liệu.")

            if idx % 50 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
            else:
                time.sleep(1)
        except Exception as e:
            print(f" -> [{ticker}] Lỗi: {e}")

    print("Hoàn tất đẩy financial ratios lên MinIO.")

if __name__ == "__main__":
    fetch_financial_ratio()
