# FILE: main.py
import json
import time
import threading
import pandas as pd
import s3fs
from fastapi import FastAPI, Body
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from pydantic import BaseModel
from google import genai
from google.api_core import exceptions

app = FastAPI()

# --- [CẤU HÌNH API KEY] ---
GOOGLE_API_KEY = "AIzaSyDBGkUOid-L9im5hBkvbI-XAVAKGHokC3c"

# FIX: Không ép api_version='v1' nữa, để mặc định để tránh lỗi 404
client = genai.Client(api_key=GOOGLE_API_KEY)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# --- KẾT NỐI HẠ TẦNG ---
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

# --- XỬ LÝ LỊCH SỬ ---
def resample_ohlc(df, interval):
    mapping = {'1m': '1min', '15m': '15min', '1h': '1h', '4h': '4h', '1d': '1D'}
    rule = mapping.get(interval, '15min')
    if 'timestamp' in df.columns: df = df.set_index('timestamp')
    if 'open' in df.columns:
        agg = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
    else:
        agg = {'price': [('open', 'first'), ('high', 'max'), ('low', 'min'), ('close', 'last')]}
    df_res = df.resample(rule).agg(agg)
    if 'open' not in df.columns: df_res.columns = df_res.columns.droplevel(0)
    return df_res.dropna().reset_index()

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
            df_final = resample_ohlc(df, interval)
            df_final = df_final.sort_values('timestamp')
            df_final['time'] = df_final['timestamp'].astype('int64') // 10**9
            return df_final[['time', 'open', 'high', 'low', 'close']].to_dict(orient='records')
    except: return []

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    if REALTIME_CACHE["price"] == 0: return []
    return [{"time": int(REALTIME_CACHE["timestamp"] / 1000), "close": REALTIME_CACHE["price"]}]

# --- AI CHAT BOT ---
class ChatRequest(BaseModel): message: str

@app.post("/api/chat")
async def chat_ai(request: ChatRequest):
    current_price = REALTIME_CACHE['price']
    prompt = f"Bạn là Senior Crypto Analyst. BTC hiện tại: ${current_price:,.2f}. Câu hỏi: '{request.message}'. Trả lời ngắn gọn tiếng Việt."
    
    try:
        # Sử dụng model 1.5 Pro - Nếu vẫn 404 thì đổi sang 'gemini-1.5-flash'
        response = client.models.generate_content(
            model="gemini-1.5-flash", 
            contents=prompt
        )
        return {"response": response.text}
    except Exception as e:
        print(f"❌ LỖI AI: {str(e)}")
        # Trả lời dựa trên logic giá nếu AI lỗi
        msg = "đang giữ giá tốt" if current_price > 96000 else "đang chịu áp lực bán"
        return {"response": f"BTC đang {msg} tại mức ${current_price:,.2f}. (AI đang cập nhật dữ liệu...)"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)