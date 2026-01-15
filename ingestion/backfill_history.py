import pandas as pd
from binance.client import Client
import s3fs
import time
from datetime import datetime

# --- CAU HINH MINIO ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# Ket noi S3 va Binance (Khong can API Key neu lay data public)
client = Client()
fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    client_kwargs={'endpoint_url': MINIO_ENDPOINT}
)

def backfill_data(symbol, interval, lookback):
    print(f"--- Bat dau tai du lieu {symbol} ({lookback}) ---")
    start_time = time.time()
    
    # 1. Tai tu Binance
    # Ham nay se tu dong chia nho request de lay duoc 1 nam
    klines = client.get_historical_klines(symbol, interval, lookback)
    
    print(f"-> Da tai xong {len(klines)} cay nen tu Binance. Dang xu ly...")
    
    # 2. Chuyen doi sang DataFrame
    data = []
    for k in klines:
        data.append({
            "symbol": symbol,
            "event_time": int(k[0]),      # Thoi gian mo nen
            "price": float(k[4]),         # Gia dong cua (Close)
            "volume": float(k[5]),        # Khoi luong
            "timestamp": pd.to_datetime(int(k[0]), unit='ms')
        })
    
    df = pd.DataFrame(data)
    
    # 3. Luu xuong MinIO (Ghi de file cu)
    save_path = f"s3://bronze/coin_prices/history_{symbol}.parquet"
    
    with fs.open(save_path, 'wb') as f:
        df.to_parquet(f)
        
    duration = time.time() - start_time
    print(f"HOAN THANH: Da luu {len(df)} dong vao {save_path} (Mat {duration:.2f}s)\n")

if __name__ == "__main__":
    # Lay du lieu 1 nam cho ca 3 dong coin pho bien
    coins = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    print("=== BACKFILL DU LIEU 1 NAM ===\n")
    
    for coin in coins:
        try:
            # interval=1m, lookback="1 year ago UTC"
            backfill_data(coin, Client.KLINE_INTERVAL_1MINUTE, "365 days ago UTC")
        except Exception as e:
            print(f"Loi khi tai {coin}: {e}")
            
    print("=== TAT CA DA HOAN TAT! HE THONG SAN SANG ===")