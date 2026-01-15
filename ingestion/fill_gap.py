# FILE: ingestion/fill_gap.py
import requests
import pandas as pd
import s3fs
import time
from datetime import datetime, timedelta

# Cấu hình
SYMBOL = "BTCUSDT"
BINANCE_API = "https://api.binance.com/api/v3/klines"
MINIO_OPTS = {
    'key': 'admin', 'secret': 'password123',
    'client_kwargs': {'endpoint_url': 'http://localhost:9000'}
}

def get_minio_fs():
    return s3fs.S3FileSystem(**MINIO_OPTS)

def get_last_timestamp(fs):
    """Tìm thời điểm cuối cùng có dữ liệu trong MinIO"""
    path = f"s3://bronze/coin_prices/history_{SYMBOL}.parquet"
    if fs.exists(path):
        with fs.open(path, 'rb') as f:
            df = pd.read_parquet(f)
            # Lấy timestamp lớn nhất
            last_time = pd.to_datetime(df['timestamp']).max()
            print(f"Dữ liệu hiện tại kết thúc lúc: {last_time}")
            return df, int(last_time.timestamp() * 1000)
    else:
        print("Chưa có lịch sử, sẽ tải mới hoàn toàn.")
        return pd.DataFrame(), int((time.time() - 31536000) * 1000) # 1 năm trước

def fetch_binance_gap(start_ts):
    """Tải dữ liệu từ start_ts đến hiện tại"""
    all_data = []
    current_start = start_ts
    
    print(f"Bắt đầu tải Gap từ {datetime.fromtimestamp(start_ts/1000)}...")
    
    while True:
        params = {
            'symbol': SYMBOL, 'interval': '1m', 
            'startTime': current_start, 'limit': 1000
        }
        res = requests.get(BINANCE_API, params=params)
        data = res.json()
        
        if not data: break
        
        # Parse dữ liệu
        for candle in data:
            # [time, open, high, low, close, vol, ...]
            all_data.append({
                'symbol': SYMBOL,
                'price': float(candle[4]), # Lấy giá đóng cửa
                'volume': float(candle[5]),
                'timestamp': datetime.fromtimestamp(candle[0]/1000)
            })
            
        # Kiểm tra xem đã đến hiện tại chưa
        last_candle_time = data[-1][0]
        if last_candle_time >= (time.time() * 1000) - 60000: # Cách hiện tại 1 phút
            break
            
        current_start = last_candle_time + 60000 # Cộng 1 phút
        time.sleep(0.1) # Tránh bị ban IP
        
    print(f"Đã tải thêm {len(all_data)} cây nến.")
    return pd.DataFrame(all_data)

def main():
    fs = get_minio_fs()
    
    # 1. Đọc dữ liệu cũ
    df_old, last_ts = get_last_timestamp(fs)
    
    # 2. Tải dữ liệu bù (Gap)
    # Cộng thêm 1 phút để không trùng lặp
    df_gap = fetch_binance_gap(last_ts + 60000)
    
    if df_gap.empty:
        print("Không có Gap nào cần lấp!")
        return

    # 3. Gộp và Lưu lại
    if not df_old.empty:
        df_final = pd.concat([df_old, df_gap])
    else:
        df_final = df_gap
        
    # Xóa trùng lặp (đề phòng)
    df_final = df_final.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
    
    # Ghi đè file lịch sử
    path = f"s3://bronze/coin_prices/history_{SYMBOL}.parquet"
    with fs.open(path, 'wb') as f:
        df_final.to_parquet(f)
        
    print(f"Đã cập nhật lịch sử! Tổng cộng: {len(df_final)} dòng.")

if __name__ == "__main__":
    main()