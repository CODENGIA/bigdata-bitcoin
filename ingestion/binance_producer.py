import json
import os
import time
import websocket
import threading
from kafka import KafkaProducer
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import s3fs
from datetime import datetime, timezone

# --- CONFIG (Tương thích cả K8s và Local) ---
SYMBOL = 'btcusdt'
KAFKA_TOPIC = 'coin-ticker'

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')

WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@aggTrade"

MINIO_OPTS = {
    'key': 'admin', 'secret': 'password123',
    'client_kwargs': {'endpoint_url': MINIO_ENDPOINT}
}

print(f"--- CONFIG ---")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"MinIO: {MINIO_ENDPOINT}")

# --- KAFKA PRODUCER ---
producer = None
while producer is None:
    try:
        print(f"Đang thử kết nối Kafka tại: {KAFKA_BOOTSTRAP_SERVERS}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=10,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        print("✅ Kafka Producer Connected!")
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        print("⏳ Đang chờ 5s để thử lại...")
        time.sleep(5)

# --- FILL GAP LOGIC ---
def create_session():
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fill_gap():
    """Tự động tải dữ liệu lịch sử nếu thiếu"""
    print(">>> Đang kiểm tra dữ liệu lịch sử...")
    try:
        fs = s3fs.S3FileSystem(**MINIO_OPTS)
        path = f"s3://bronze/coin_prices/history_{SYMBOL.upper()}.parquet"
        
        start_ms = None
        
        # Kiểm tra file/folder tồn tại chưa
        if fs.exists(path):
            try:
                # Đọc parquet (Hỗ trợ cả File đơn và Folder Dataset)
                df = pd.read_parquet(path, filesystem=fs)
                
                last_ts = pd.to_datetime(df['timestamp']).max()
                start_ms = int(last_ts.value / 10**6) + 60000
                print(f"Đã có dữ liệu đến: {last_ts}")
            except Exception as e:
                print(f"File lỗi hoặc rỗng ({e}), sẽ tải lại từ đầu.")
                start_ms = None
        else:
            print("Chưa có file lịch sử. Sẽ tải mới từ 01/01/2025.")
            start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            start_ms = int(start_date.timestamp() * 1000)

        if start_ms is None:
             start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
             start_ms = int(start_date.timestamp() * 1000)

        now_ms = int(time.time() * 1000)
        
        if now_ms - start_ms < 120000:
            print("Dữ liệu đã đồng bộ.")
            return

        print(f"⬇Đang tải bù Gap ({ (now_ms - start_ms)/60000:.0f} phút)...")
        session = create_session()
        all_candles = []
        current_start = start_ms

        while True:
            url = "https://api.binance.com/api/v3/klines"
            params = {'symbol': SYMBOL.upper(), 'interval': '1m', 'startTime': current_start, 'limit': 1000}
            try:
                res = session.get(url, params=params).json()
            except Exception as req_err:
                print(f"Lỗi request Binance: {req_err}")
                time.sleep(1)
                continue
            
            if not res or not isinstance(res, list): 
                break 
            
            for c in res:
                all_candles.append({
                    'symbol': SYMBOL.upper(),
                    'price': float(c[4]),
                    'volume': float(c[5]),
                    'timestamp': pd.to_datetime(c[0], unit='ms')
                })
            
            last_candle_time = res[-1][0]
            if last_candle_time >= now_ms - 60000: break
            
            current_start = last_candle_time + 60000
            time.sleep(0.1)
            print(f"Đã tải đến: {pd.to_datetime(last_candle_time, unit='ms')}", end='\r')

        if all_candles:
            print("\nĐang lưu file Parquet vào MinIO...")
            df_gap = pd.DataFrame(all_candles)
            
            final_df = df_gap
            if fs.exists(path):
                try:
                    # Đọc lại dữ liệu cũ để merge
                    df_old = pd.read_parquet(path, filesystem=fs)
                    final_df = pd.concat([df_old, df_gap]).drop_duplicates(subset=['timestamp']).sort_values('timestamp')
                except:
                    pass
            
            # --- KHẮC PHỤC LỖI FOLDER/FILE TẠI ĐÂY ---
            # Thay vì ghi đè lên 'path' (khiến nó thành File đơn),
            # ta ghi vào 'path/init.parquet'.
            # Điều này biến 'history_BTCUSDT.parquet' thành FOLDER.
            
            # Xóa path cũ nếu nó đang là file đơn (để tránh lỗi IsADirectoryError/NotADirectoryError)
            try:
                file_info = fs.info(path)
                if file_info['type'] == 'file':
                    print("⚠️ Phát hiện file đơn cũ, đang xóa để chuyển sang cấu trúc folder...")
                    fs.rm(path)
            except:
                pass # Path chưa tồn tại hoặc lỗi khác

            # Ghi vào file con bên trong folder
            save_path = f"{path}/init.parquet"
            with fs.open(save_path, 'wb') as f:
                final_df.to_parquet(f)
                
            print(f"✅ Đã cập nhật lịch sử thành công vào: {save_path}")
            
    except Exception as e:
        print(f"Lỗi Fill Gap: {e}")

# --- WEBSOCKET LOGIC ---
def on_message(ws, message):
    try:
        data = json.loads(message)
        payload = {
            "symbol": SYMBOL.upper(),
            "price": float(data['p']),
            "volume": float(data['q']),
            "event_time": data['T']
        }
        producer.send(KAFKA_TOPIC, payload)
        if int(time.time()) % 5 == 0: 
            print(f"📡 Live Price: {payload['price']}", end='\r')
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WS Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WS Closed. Reconnecting in 2s...")
    time.sleep(2)
    start_socket()

def start_socket():
    ws = websocket.WebSocketApp(
        WS_URL, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    fill_gap()
    print("\nSTARTING REALTIME STREAM TO KAFKA...")
    start_socket()