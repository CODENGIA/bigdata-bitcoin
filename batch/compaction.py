import os
import pandas as pd
import s3fs
from sqlalchemy import create_engine, text
import warnings

warnings.filterwarnings("ignore")

# --- CONFIG ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

# Ưu tiên dùng DATABASE_URL nếu có, nếu không thì lắp ghép
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    PG_USER = os.getenv("PG_USER", "user") # Khớp với app-secrets
    PG_PASS = os.getenv("PG_PASS", "password")
    PG_DB = os.getenv("PG_DB", "crypto_db")
    DB_HOST = "postgres" if os.getenv("KUBERNETES_SERVICE_HOST") else "localhost"
    DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@{DB_HOST}:5432/{PG_DB}"

fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY, 
    secret=MINIO_SECRET_KEY,
    client_kwargs={'endpoint_url': MINIO_ENDPOINT}
)

def process_symbol(symbol):
    print(f"\n[START] Đang xử lý: {symbol}")
    bronze_path_glob = f"bronze/coin_prices/history_{symbol}.parquet/*.parquet"
    silver_path = f"s3://silver/coin_prices/{symbol}_clean.parquet"

    try:
        files = fs.glob(bronze_path_glob)
        if not files:
            print(f"   - Không tìm thấy dữ liệu Bronze.")
            return

        dfs = []
        for file_path in files:
            try:
                with fs.open(file_path, 'rb') as f:
                    dfs.append(pd.read_parquet(f))
            except: pass

        if not dfs: return

        df = pd.concat(dfs, ignore_index=True)
        total_raw = len(df)
        
        keep_cols = ['timestamp', 'price', 'volume', 'symbol']
        available_cols = [c for c in keep_cols if c in df.columns]
        df = df[available_cols]

        df_clean = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        total_clean = len(df_clean)
        
        with fs.open(silver_path, 'wb') as f:
            df_clean.to_parquet(f)
        print(f"   - SILVER DONE: {total_raw} -> {total_clean} dòng.")

        # --- GIAI ĐOẠN GOLD ---
        if DATABASE_URL and not df_clean.empty:
            try:
                print("   - Đang tính toán KPI cho Gold Layer...")
                last_date = df_clean['timestamp'].max().strftime('%Y-%m-%d')
                
                stats = {
                    'date': last_date,
                    'symbol': symbol,
                    'high_price': float(df_clean['price'].max()),
                    'low_price': float(df_clean['price'].min()),
                    'avg_price': float(df_clean['price'].mean()),
                    'volatility': float(df_clean['price'].std()),
                    'updated_at': pd.Timestamp.now()
                }
                
                df_stats = pd.DataFrame([stats])
                engine = create_engine(DATABASE_URL)
                
                # SỬA LỖI: Sử dụng text() và bọc trong transaction
                with engine.begin() as conn:
                    delete_query = text("DELETE FROM daily_stats WHERE symbol = :sym AND date = :dt")
                    conn.execute(delete_query, {"sym": symbol, "dt": last_date})
                
                df_stats.to_sql('daily_stats', engine, if_exists='append', index=False)
                print(f"   - GOLD DONE: High ${stats['high_price']} | Low ${stats['low_price']}")
                
            except Exception as e:
                print(f"   - Lỗi ghi Gold Layer: {e}")

    except Exception as e:
        print(f"Lỗi xử lý {symbol}: {e}")

def compact_data_all():
    print(">>> BẮT ĐẦU BATCH COMPACTION...")
    for sym in ["BTCUSDT", "ETHUSDT"]:
        process_symbol(sym)
    print("\nBATCH JOB HOÀN TẤT TOÀN BỘ.")

if __name__ == "__main__":
    compact_data_all()