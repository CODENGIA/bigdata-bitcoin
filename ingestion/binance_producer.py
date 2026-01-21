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

# --- CONFIG ĐA COIN ---
# 1. Danh sách coin cần theo dõi (Chữ thường)
SYMBOLS = ['btcusdt', 'ethusdt'] 
KAFKA_TOPIC = 'coin-ticker'

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')

# 2. Tạo URL Combined Stream để nghe nhiều coin cùng lúc
# Format: stream?streams=btcusdt@aggTrade/ethusdt@aggTrade
stream_params = "/".join([f"{s.lower()}@aggTrade" for s in SYMBOLS])
WS_URL = f"wss://stream.binance.com:9443/stream?streams={stream_params}"

MINIO_OPTS = {
    'key': os.getenv('MINIO_ACCESS_KEY', 'admin'),
    'secret': os.getenv('MINIO_SECRET_KEY', 'password123'),
    'client_kwargs': {'endpoint_url': MINIO_ENDPOINT}
}

print(f"--- CONFIG ---")
print(f"Symbols: {SYMBOLS}")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"MinIO: {MINIO_ENDPOINT}")
print(f"WS URL: {WS_URL}")

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

# --- FILL GAP LOGIC (Hỗ trợ đa Coin) ---
def create_session():
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fill_gap_for_symbol(symbol):
    """Tự động tải dữ liệu lịch sử cho MỘT đồng coin"""
    symbol_upper = symbol.upper()
    print(f"\n>>> [{symbol_upper}] Đang kiểm tra dữ liệu lịch sử...")
    
    try:
        fs = s3fs.S3FileSystem(**MINIO_OPTS)
        # Đường dẫn folder cho từng coin
        path = f"s3://bronze/coin_prices/history_{symbol_upper}.parquet"
        
        start_ms = None
        
        # Kiểm tra file/folder tồn tại chưa
        if fs.exists(path):
            try:
                # Đọc parquet
                df = pd.read_parquet(path, filesystem=fs)
                last_ts = pd.to_datetime(df['timestamp']).max()
                start_ms = int(last_ts.value / 10**6) + 60000
                print(f"   Đã có dữ liệu đến: {last_ts}")
            except Exception as e:
                print(f"   File lỗi hoặc rỗng ({e}), sẽ tải lại từ đầu.")
                start_ms = None
        else:
            print(f"   Chưa có file lịch sử. Sẽ tải mới từ 01/01/2025.")
            start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            start_ms = int(start_date.timestamp() * 1000)

        if start_ms is None:
             start_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
             start_ms = int(start_date.timestamp() * 1000)

        now_ms = int(time.time() * 1000)
        
        if now_ms - start_ms < 120000:
            print(f"   ✅ Dữ liệu {symbol_upper} đã đồng bộ.")
            return

        print(f"   ⬇ Đang tải bù Gap ({ (now_ms - start_ms)/60000:.0f} phút)...")
        session = create_session()
        all_candles = []
        current_start = start_ms

        while True:
            url = "https://api.binance.com/api/v3/klines"
            params = {'symbol': symbol_upper, 'interval': '1m', 'startTime': current_start, 'limit': 1000}
            try:
                res = session.get(url, params=params).json()
            except Exception as req_err:
                print(f"   Lỗi request Binance: {req_err}")
                time.sleep(1)
                continue
            
            if not res or not isinstance(res, list): 
                break 
            
            for c in res:
                all_candles.append({
                    'symbol': symbol_upper,
                    'price': float(c[4]),
                    'volume': float(c[5]),
                    'timestamp': pd.to_datetime(c[0], unit='ms')
                })
            
            last_candle_time = res[-1][0]
            if last_candle_time >= now_ms - 60000: break
            
            current_start = last_candle_time + 60000
            # Giảm log spam
            if len(all_candles) % 10000 == 0:
                print(f"   Đã tải đến: {pd.to_datetime(last_candle_time, unit='ms')}", end='\r')

        if all_candles:
            print(f"\n   💾 Đang lưu file Parquet {symbol_upper} vào MinIO...")
            df_gap = pd.DataFrame(all_candles)
            
            final_df = df_gap
            if fs.exists(path):
                try:
                    df_old = pd.read_parquet(path, filesystem=fs)
                    final_df = pd.concat([df_old, df_gap]).drop_duplicates(subset=['timestamp']).sort_values('timestamp')
                except:
                    pass
            
            # Xóa file đơn cũ nếu có (để chuyển sang folder structure)
            try:
                file_info = fs.info(path)
                if file_info['type'] == 'file':
                    fs.rm(path)
            except:
                pass

            # Ghi vào init.parquet
            save_path = f"{path}/init.parquet"
            with fs.open(save_path, 'wb') as f:
                final_df.to_parquet(f)
                
            print(f"   ✅ Đã cập nhật xong: {save_path}")
            
    except Exception as e:
        print(f"❌ Lỗi Fill Gap {symbol_upper}: {e}")

# --- WEBSOCKET LOGIC ---
def on_message(ws, message):
    try:
        raw_data = json.loads(message)
        
        # Khi dùng Combined Stream, cấu trúc JSON sẽ là:
        # {"stream": "btcusdt@aggTrade", "data": {...nội dung cũ...}}
        if 'data' in raw_data:
            data = raw_data['data']
        else:
            data = raw_data

        payload = {
            "symbol": data['s'],  # Lấy Symbol thực tế từ message (BTCUSDT, ETHUSDT)
            "price": float(data['p']),
            "volume": float(data['q']),
            "event_time": data['T']
        }
        
        producer.send(KAFKA_TOPIC, payload)
        
        # Log mẫu (chỉ in BTC để đỡ loạn)
        if payload['symbol'] == 'BTCUSDT' and int(time.time()) % 5 == 0: 
            print(f"📡 [{payload['symbol']}] Price: {payload['price']}", end='\r')
            
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WS Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WS Closed. Reconnecting in 2s...")
    time.sleep(2)
    start_socket()

def start_socket():
    # WebsocketApp hỗ trợ URL có query param (combined streams)
    ws = websocket.WebSocketApp(
        WS_URL, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    # 1. Chạy Fill Gap cho từng Coin trong danh sách
    print("--- STARTING HISTORICAL SYNC ---")
    for sym in SYMBOLS:
        fill_gap_for_symbol(sym)
    
    # 2. Bắt đầu nghe Realtime cho tất cả Coin
    print(f"\n--- STARTING REALTIME STREAM ({len(SYMBOLS)} coins) ---")
    start_socket()