# FILE: main.py
import json
import time
import threading
import pandas as pd
import s3fs
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from pydantic import BaseModel
from google import genai

app = FastAPI()

# --- CẤU HÌNH API KEY ---
GOOGLE_API_KEY = "AIzaSyDBGkUOid-L9im5hBkvbI-XAVAKGHokC3c"
# Khởi tạo Client theo chuẩn mới
client = genai.Client(api_key=GOOGLE_API_KEY)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

fs = s3fs.S3FileSystem(
    key="admin", secret="password123", client_kwargs={'endpoint_url': 'http://localhost:9000'}
)

REALTIME_CACHE = {"symbol": "BTCUSDT", "price": 0, "timestamp": 0}

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
                REALTIME_CACHE["timestamp"] = data["event_time"]
        except:
            time.sleep(5)

threading.Thread(target=kafka_listener, daemon=True).start()

# --- XỬ LÝ LỊCH SỬ & FIX LỖI THỜI GIAN ---
def resample_ohlc(df, interval):
    # Mapping giây để tính nến hiện tại
    interval_seconds = {'1m': 60, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
    sec = interval_seconds.get(interval, 900)
    
    mapping = {'1m': '1min', '15m': '15min', '1h': '1h', '4h': '4h', '1d': '1D'}
    rule = mapping.get(interval, '15min')

    if 'timestamp' in df.columns: df = df.set_index('timestamp')
    
    agg = {'price': [('open', 'first'), ('high', 'max'), ('low', 'min'), ('close', 'last')]}
    df_res = df.resample(rule).agg(agg)
    df_res.columns = df_res.columns.droplevel(0)
    df_res = df_res.dropna().reset_index()
    df_res['time'] = df_res['timestamp'].astype('int64') // 10**9
    
    last_history_time = df_res['time'].iloc[-1] if not df_res.empty else 0
    current_ts = REALTIME_CACHE['timestamp'] / 1000 if REALTIME_CACHE['timestamp'] > 0 else time.time()
    current_price = REALTIME_CACHE['price']
    
    # Tính thời gian bắt đầu của nến đang chạy
    current_candle_start = int(current_ts // sec) * sec
    
    if current_candle_start >= last_history_time and current_price > 0:
        if current_candle_start == last_history_time:
             # Update nến cuối nếu trùng thời gian
             df_res.iloc[-1, df_res.columns.get_loc('close')] = current_price
             df_res.iloc[-1, df_res.columns.get_loc('high')] = max(df_res.iloc[-1]['high'], current_price)
             df_res.iloc[-1, df_res.columns.get_loc('low')] = min(df_res.iloc[-1]['low'], current_price)
        else:
            # Tạo nến mới nếu đã sang khung giờ mới
            new_candle = {
                'timestamp': pd.to_datetime(current_candle_start, unit='s'),
                'open': current_price, 'high': current_price, 'low': current_price, 'close': current_price, 
                'time': current_candle_start
            }
            df_res = pd.concat([df_res, pd.DataFrame([new_candle])], ignore_index=True)

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
    except: return []

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    if REALTIME_CACHE["price"] == 0: return []
    return [{"time": int(REALTIME_CACHE["timestamp"] / 1000), "close": REALTIME_CACHE["price"]}]

class ChatRequest(BaseModel): message: str

@app.post("/api/chat")
async def chat_ai(request: ChatRequest):
    price = REALTIME_CACHE['price']
    prompt = f"Giá BTC hiện tại: ${price:,.2f}. Câu hỏi: {request.message}. Trả lời ngắn gọn tiếng Việt."

    # Danh sách model có trong list của bạn (Ưu tiên Flash 2.0 cho nhanh)
    models_to_try = ["gemini-2.5-flash", "gemini-2.0-flash-exp", "gemini-flash-latest"]
    
    for model_name in models_to_try:
        try:
            # GỌI THƯ VIỆN GENAI MỚI
            response = client.models.generate_content(
                model=model_name, 
                contents=prompt
            )
            return {"response": response.text}
        except Exception as e:
            print(f"⚠️ Model {model_name} lỗi: {e}")
            continue

    # FALLBACK: Nếu AI toang hết thì tự trả lời bằng logic (để Web không bị đơ)
    trend = "tăng" if price > 96000 else "giảm"
    return {"response": f"BTC đang {trend} nhẹ quanh mức ${price:,.2f}. (AI đang quá tải, vui lòng thử lại sau)."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)