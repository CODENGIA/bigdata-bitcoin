# FILE: main.py
import json
import time
import threading
import pandas as pd
import s3fs
import numpy as np
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from pydantic import BaseModel
from google import genai

app = FastAPI()

# --- CẤU HÌNH API KEY ---
GOOGLE_API_KEY = "AIzaSyDBGkUOid-L9im5hBkvbI-XAVAKGHokC3c"
client = genai.Client(api_key=GOOGLE_API_KEY)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

fs = s3fs.S3FileSystem(
    key="admin", secret="password123", client_kwargs={'endpoint_url': 'http://localhost:9000'}
)

# Cache lưu giá và thời gian mới nhất từ Kafka
REALTIME_CACHE = {
    "symbol": "BTCUSDT",
    "price": 0,
    "timestamp": 0
}

def kafka_listener():
    while True:
        try:
            consumer = KafkaConsumer(
                'coin-ticker',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("✅ API: Đã kết nối Kafka!")
            for message in consumer:
                data = message.value
                REALTIME_CACHE["price"] = data["price"]
                REALTIME_CACHE["timestamp"] = data["event_time"] # Thời gian Unix ms
        except:
            time.sleep(5)

threading.Thread(target=kafka_listener, daemon=True).start()

# --- XỬ LÝ LỊCH SỬ VÀ ĐIỀN NẾN HIỆN TẠI ---
def resample_ohlc(df, interval):
    # Quy đổi interval sang giây để tính toán
    interval_seconds = {'1m': 60, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
    sec = interval_seconds.get(interval, 900)
    
    mapping = {'1m': '1min', '15m': '15min', '1h': '1h', '4h': '4h', '1d': '1D'}
    rule = mapping.get(interval, '15min')

    if 'timestamp' in df.columns: df = df.set_index('timestamp')
    
    # Resample dữ liệu lịch sử
    agg = {'price': [('open', 'first'), ('high', 'max'), ('low', 'min'), ('close', 'last')]}
    df_res = df.resample(rule).agg(agg)
    df_res.columns = df_res.columns.droplevel(0)
    df_res = df_res.dropna().reset_index()
    
    # Convert sang timestamp (giây)
    df_res['time'] = df_res['timestamp'].astype('int64') // 10**9
    
    # --- LOGIC FIX LỖI TIME ---
    # Lấy nến cuối cùng từ lịch sử
    last_history_time = df_res['time'].iloc[-1] if not df_res.empty else 0
    
    # Lấy thời gian hiện tại thực tế (Kafka hoặc System)
    current_ts = REALTIME_CACHE['timestamp'] / 1000 if REALTIME_CACHE['timestamp'] > 0 else time.time()
    current_price = REALTIME_CACHE['price']

    # Tính toán thời gian bắt đầu của nến hiện tại (ví dụ: bây giờ là 21:05 -> nến 15m bắt đầu lúc 21:00)
    current_candle_start = int(current_ts // sec) * sec
    
    # Nếu nến hiện tại chưa có trong lịch sử -> Tạo giả nến đó để chart không bị đứng
    if current_candle_start > last_history_time and current_price > 0:
        new_candle = {
            'timestamp': pd.to_datetime(current_candle_start, unit='s'),
            'open': current_price, # Tạm lấy giá hiện tại làm Open
            'high': current_price,
            'low': current_price,
            'close': current_price,
            'time': current_candle_start
        }
        # Nếu nến cuối cùng của lịch sử chính là nến đang chạy (nhưng chưa đóng), update giá close
        if last_history_time == current_candle_start:
             df_res.iloc[-1, df_res.columns.get_loc('close')] = current_price
             df_res.iloc[-1, df_res.columns.get_loc('high')] = max(df_res.iloc[-1]['high'], current_price)
             df_res.iloc[-1, df_res.columns.get_loc('low')] = min(df_res.iloc[-1]['low'], current_price)
        else:
            # Append nến mới vào DataFrame
            df_new = pd.DataFrame([new_candle])
            df_res = pd.concat([df_res, df_new], ignore_index=True)

    return df_res[['time', 'open', 'high', 'low', 'close']].to_dict(orient='records')

@app.get("/")
async def read_index(): return FileResponse('index.html')

@app.get("/api/history/{symbol}")
def get_history(symbol: str, interval: str = '15m'):
    try:
        path = f"s3://bronze/coin_prices/history_{symbol}.parquet"
        if fs.exists(path):
            with fs.open(path, 'rb') as f:
                df = pd.read_parquet(f)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return resample_ohlc(df, interval)
    except Exception as e: 
        print(f"Lỗi đọc file: {e}")
        return []

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    if REALTIME_CACHE["price"] == 0: return []
    return [{"time": int(REALTIME_CACHE["timestamp"] / 1000), "close": REALTIME_CACHE["price"]}]

class ChatRequest(BaseModel): message: str

@app.post("/api/chat")
async def chat_ai(request: ChatRequest):
    price = REALTIME_CACHE['price']
    prompt = f"Gia BTC: ${price:,.2f}. Cau hoi: {request.message}. Tra loi ngan gon duoi 30 chu tieng Viet."
    priority_models = ["gemini-3-pro-preview", "gemini-2.5-pro", "gemini-2.0-flash"]
    for model_name in priority_models:
        try:
            response = client.models.generate_content(model=model_name, contents=prompt)
            return {"response": response.text}
        except: continue
    msg = "dang giu gia tot" if price > 96000 else "dang dieu chinh nhe"
    return {"response": f"BTC {msg} quanh ${price:,.2f}. AI dang ban."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)