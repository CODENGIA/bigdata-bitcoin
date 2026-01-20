import os
import pandas as pd
import s3fs
import pyarrow.parquet as pq

# Config
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
fs = s3fs.S3FileSystem(
    key="admin", secret="password123",
    client_kwargs={'endpoint_url': MINIO_ENDPOINT}
)

def compact_data():
    symbol = "BTCUSDT"
    # Lưu ý: S3FS glob không cần prefix s3:// khi dùng với object fs
    bronze_path_glob = f"bronze/coin_prices/history_{symbol}.parquet/*.parquet"
    silver_path = f"s3://silver/coin_prices/{symbol}_clean.parquet"

    print(">>> BẮT ĐẦU BATCH COMPACTION...")
    
    try:
        # 1. Liệt kê tất cả file parquet trong folder (Init + Part files)
        files = fs.glob(bronze_path_glob)
        
        if not files:
            print(f"Không tìm thấy file nào trong: {bronze_path_glob}")
            return

        print(f"Tìm thấy {len(files)} file thành phần. Đang đọc và gộp...")

        # 2. Đọc từng file và gom vào list
        dfs = []
        for file_path in files:
            try:
                # Mở từng file bằng fs.open để tránh lỗi đường dẫn
                with fs.open(file_path, 'rb') as f:
                    dfs.append(pd.read_parquet(f))
            except Exception as e:
                print(f"Lỗi đọc file con {file_path}: {e}")

        if not dfs:
            print("Không đọc được dữ liệu nào.")
            return

        # 3. Gộp thành 1 DataFrame to (Concat)
        df = pd.concat(dfs, ignore_index=True)
        print(f"Tổng cộng: {len(df)} dòng dữ liệu thô.")
        
        # 4. Xử lý dữ liệu (Batch Processing)
        # - Loại bỏ trùng lặp (Deduplicate)
        # - Sắp xếp theo thời gian
        df_clean = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        
        print(f"Sau khi làm sạch: {len(df_clean)} dòng.")
        
        # 5. Ghi sang Silver
        with fs.open(silver_path, 'wb') as f:
            df_clean.to_parquet(f)
            
        print(f"Đã ghi dữ liệu sạch sang: {silver_path}")
        print("Batch Job hoàn tất. Dữ liệu Silver đã sẵn sàng cho LLM Training.")
        
    except Exception as e:
        print(f"Lỗi Compaction: {e}")

if __name__ == "__main__":
    compact_data()