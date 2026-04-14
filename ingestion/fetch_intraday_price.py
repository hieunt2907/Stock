import time
import pandas as pd
from vnstock import Quote
from config import get_vn30_tickers, upload_dataframe_to_minio
from schema_validator import validate_dataframe

TOPIC = 'intraday_price'

def fetch_intraday():
    tickers = get_vn30_tickers()

    print(f"Đang lấy dữ liệu intraday cho {len(tickers)} tickers VN30...")
    for idx, ticker in enumerate(tickers, 1):
        try:
            quote        = Quote(symbol=ticker, source='VCI')
            df_intraday  = quote.history(interval='5m')

            if not df_intraday.empty:
                df_intraday['ticker'] = ticker
                df_intraday['type']   = TOPIC

                # --- Schema Validation ---
                is_valid, report = validate_dataframe(df_intraday, topic=TOPIC, ticker=ticker)
                if not is_valid:
                    print(f" -> [{ticker}] Bỏ qua upload do schema không hợp lệ: {report}")
                    continue

                ok = upload_dataframe_to_minio(df_intraday, topic=TOPIC, partition_key=ticker)
                if ok:
                    print(f" -> [{ticker}] Upload thành công {len(df_intraday)} dòng intraday lên MinIO.")

            if idx % 50 == 0:
                print(f"[Rate Limit] Dừng 20s sau {idx} requests...")
                time.sleep(20)
            else:
                time.sleep(1)
        except Exception as e:
            print(f" -> [{ticker}] Lỗi: {e}")

    print("Hoàn tất upload Intraday Data lên MinIO.")

if __name__ == "__main__":
    fetch_intraday()
