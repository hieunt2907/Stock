import time
import pandas as pd
from config import get_vn30_tickers, upload_dataframe_to_minio
from schema_validator import validate_dataframe

TOPIC = 'ticker_list'

def fetch_vn30_tickers_to_minio():
    print("Đang lấy danh sách tickers VN30 từ nguồn KBS...")
    try:
        vn30_symbols = get_vn30_tickers()

        records = [
            {'ticker': symbol, 'group': 'VN30', 'type': TOPIC}
            for symbol in vn30_symbols
        ]
        df = pd.DataFrame(records)

        # --- Schema Validation ---
        is_valid, report = validate_dataframe(df, topic=TOPIC)
        if not is_valid:
            print(f"Bỏ qua upload do schema không hợp lệ: {report}")
            return vn30_symbols

        ok = upload_dataframe_to_minio(df, topic=TOPIC, partition_key='VN30')
        if ok:
            print(f"Đã upload {len(vn30_symbols)} tickers VN30 lên MinIO.")

        return vn30_symbols
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu tickers: {e}")

if __name__ == "__main__":
    fetch_vn30_tickers_to_minio()
