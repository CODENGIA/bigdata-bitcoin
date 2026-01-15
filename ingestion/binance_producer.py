import json
import time
import websocket
import threading
from kafka import KafkaProducer
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import s3fs
from datetime import datetime, timezone  # <--- THÊM IMPORT NÀY

# --- CONFIG ---
SYMBOL = 'btcusdt'
KAFKA_TOPIC = 'coin-ticker'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@aggTrade"
MINIO_OPTS = {
    'key': 'admin', 'secret': 'password123',
    'client_kwargs': {'endpoint_url': 'http://localhost:9000'}
}

# --- KAFKA PRODUCER ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

# --- FILL GAP LOGIC ---
def create_session():
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fill_gap():
    """Tu dong tai du lieu lich su neu thieu"""
    print(">>> Dang kiem tra du lieu lich su...")
    try:
        fs = s3fs.S3FileSystem(**MINIO_OPTS)
        path = f"s3://bronze/coin_prices/history_{SYMBOL.upper()}.parquet"
        
        start_ms = None
        if fs.exists(path):
            with fs.open(path, 'rb') as f:
                df = pd.read_parquet(f)
            # Nếu file đã tồn tại, tiếp tục tải từ thời điểm cuối cùng trong file
            last_ts = pd.to_datetime(df['timestamp']).max()
            start_ms = int(last_ts.value / 10**6) + 60000
            print(f"Lich su den: {last_ts}")
        else:
            # --- ĐOẠN CHỈNH SỬA Ở ĐÂY ---
            print("Chua co file lich su, se tai tu 01/01/2025.")
            # Tạo mốc thời gian 01/01/2025 00:00:00 UTC
            start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            # Chuyển đổi sang miliseconds
            start_ms = int(start_date.timestamp() * 1000)

        now_ms = int(time.time() * 1000)
        
        # Nếu thời gian cần tải < 2 phút so với hiện tại thì bỏ qua
        if now_ms - start_ms < 120000:
            print("Du lieu da dong bo.")
            return

        print(f"Dang tai bu Gap ({ (now_ms - start_ms)/60000 } phut)...")
        session = create_session()
        all_candles = []
        current_start = start_ms

        while True:
            url = "https://api.binance.com/api/v3/klines"
            params = {'symbol': SYMBOL.upper(), 'interval': '1m', 'startTime': current_start, 'limit': 1000}
            res = session.get(url, params=params).json()
            
            if not res or not isinstance(res, list): break # Kiểm tra kỹ hơn response
            
            for c in res:
                all_candles.append({
                    'symbol': SYMBOL.upper(),
                    'price': float(c[4]),
                    'volume': float(c[5]),
                    'timestamp': pd.to_datetime(c[0], unit='ms')
                })
            
            last_candle_time = res[-1][0]
            # Nếu nến cuối cùng đã gần sát hiện tại (cách 1 phút) thì dừng
            if last_candle_time >= now_ms - 60000: break
            
            current_start = last_candle_time + 60000
            time.sleep(0.1) # Tránh bị rate limit
            
            # In tiến độ để đỡ sốt ruột
            print(f"Da tai den: {pd.to_datetime(last_candle_time, unit='ms')}", end='\r')

        if all_candles:
            print("\nDang luu file Parquet...")
            df_gap = pd.DataFrame(all_candles)
            if fs.exists(path):
                with fs.open(path, 'rb') as f:
                    df_old = pd.read_parquet(f)
                df_final = pd.concat([df_old, df_gap]).drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            else:
                df_final = df_gap
            
            with fs.open(path, 'wb') as f:
                df_final.to_parquet(f)
            print("Da cap nhat lich su thanh cong!")
            
    except Exception as e:
        print(f"Loi Fill Gap: {e}")

# --- WEBSOCKET LOGIC ---
def on_message(ws, message):
    data = json.loads(message)
    payload = {
        "symbol": SYMBOL.upper(),
        "price": float(data['p']),
        "volume": float(data['q']),
        "event_time": data['T']
    }
    producer.send(KAFKA_TOPIC, payload)
    if int(time.time()) % 5 == 0: 
        print(f"Live: {payload['price']}", end='\r')

def on_error(ws, error):
    print(f"WS Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WS Closed. Reconnecting...")
    time.sleep(2)
    start_socket()

def start_socket():
    ws = websocket.WebSocketApp(
        WS_URL, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    fill_gap()
    print("--- STARTING REALTIME STREAM ---")
    start_socket()