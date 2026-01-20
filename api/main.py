import json
import time
import threading
import os
import pandas as pd
import s3fs
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from pydantic import BaseModel
from google import genai

# --- THÊM: Import cho Database ---
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

app = FastAPI()

# --- CẤU HÌNH HỆ THỐNG (LẤY TỪ ENV - BẢO MẬT TUYỆT ĐỐI) ---

# 1. API KEY GOOGLE
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    print("⚠️ Cảnh báo: Thiếu GOOGLE_API_KEY env var!")
    # Không để key cứng ở đây nữa, nếu thiếu thì chấp nhận lỗi để debug
    GOOGLE_API_KEY = "DUMMY_KEY"

client = genai.Client(api_key=GOOGLE_API_KEY)

# 2. Cấu hình Kafka & MinIO
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')

# 3. Cấu hình Database PostgreSQL
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB = os.getenv("PG_DB")
DATABASE_URL = os.getenv("DATABASE_URL")

# Tự động ghép thành URL nếu thiếu DATABASE_URL nhưng có đủ linh kiện
if not DATABASE_URL and PG_USER and PG_PASS and PG_DB:
    DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@postgres:5432/{PG_DB}"

print(f"--- CONFIG ---")
print(f"Kafka: {KAFKA_BOOTSTRAP}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")

if DATABASE_URL:
    print("✅ DATABASE_URL: Đã cấu hình (từ Secret)")
else:
    print("⚠️ DATABASE_URL: Thiếu (Audit Log sẽ tắt)")

# 4. Cấu hình MinIO Credentials
# SỬA: Lấy Key/Secret từ biến môi trường (được K8s bơm vào từ Secret)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")      # Fallback cho local dev
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123") # Fallback cho local dev

print(f"--- CONFIG ---")
print(f"Kafka: {KAFKA_BOOTSTRAP}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
if DATABASE_URL:
    print("✅ DATABASE_URL: Found")
else:
    print("⚠️ DATABASE_URL: Missing")

# --- KẾT NỐI DATABASE ---
engine = None
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base = declarative_base()

        class ChatLog(Base):
            __tablename__ = "chat_logs"
            id = Column(Integer, primary_key=True, index=True)
            user_id = Column(String, default="anonymous")
            message = Column(Text)
            response = Column(Text)
            model_used = Column(String)
            timestamp = Column(DateTime(timezone=True), server_default=func.now())

        Base.metadata.create_all(bind=engine)
        print("✅ Đã kết nối PostgreSQL thành công!")
    except Exception as e:
        print(f"⚠️ Không thể kết nối DB: {e}")
        engine = None
else:
    print("⚠️ Chưa cấu hình DB, tính năng Audit Log sẽ bị tắt.")
    engine = None

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# Kết nối MinIO (SỬA LỖI HARDCODE TẠI ĐÂY)
fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY,       # <--- Đã dùng biến
    secret=MINIO_SECRET_KEY,    # <--- Đã dùng biến
    client_kwargs={'endpoint_url': MINIO_ENDPOINT}
)

REALTIME_CACHE = {"symbol": "BTCUSDT", "price": 0, "timestamp": 0}

# --- KAFKA LISTENER ---
def kafka_listener():
    while True:
        try:
            consumer = KafkaConsumer(
                'coin-ticker',
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("✅ API: Đã kết nối Kafka thành công!")
            for message in consumer:
                data = message.value
                REALTIME_CACHE["price"] = data["price"]
                REALTIME_CACHE["timestamp"] = data["event_time"]
        except Exception as e:
            print(f"⚠️ Lỗi kết nối Kafka ({KAFKA_BOOTSTRAP}): {e}")
            time.sleep(5)

threading.Thread(target=kafka_listener, daemon=True).start()

# --- XỬ LÝ NẾN & FIX LỖI THỜI GIAN ---
def resample_ohlc(df, interval):
    interval_seconds = {'1m': 60, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
    sec = interval_seconds.get(interval, 900)
    mapping = {'1m': '1min', '15m': '15min', '1h': '1h', '4h': '4h', '1d': '1D'}
    rule = mapping.get(interval, '15min')

    if 'timestamp' in df.columns:
        df = df.set_index('timestamp')

    agg = {'price': [('open', 'first'), ('high', 'max'), ('low', 'min'), ('close', 'last')]}
    df_res = df.resample(rule).agg(agg)
    df_res.columns = df_res.columns.droplevel(0)
    
    # Fill Gap Logic
    df_res['close'] = df_res['close'].ffill()
    df_res['open'] = df_res['open'].fillna(df_res['close'])
    df_res['high'] = df_res['high'].fillna(df_res['close'])
    df_res['low'] = df_res['low'].fillna(df_res['close'])

    df_res['prev_close'] = df_res['close'].shift(1)
    mask = ~df_res['prev_close'].isna()
    df_res.loc[mask, 'open'] = df_res.loc[mask, 'prev_close']
    
    df_res['high'] = df_res[['high', 'open', 'close']].max(axis=1)
    df_res['low'] = df_res[['low', 'open', 'close']].min(axis=1)

    df_res = df_res.dropna().reset_index()
    df_res['time'] = df_res['timestamp'].astype('int64') // 10**9

    last_history_time = df_res['time'].iloc[-1] if not df_res.empty else 0
    current_ts = REALTIME_CACHE['timestamp'] / 1000 if REALTIME_CACHE['timestamp'] > 0 else time.time()
    current_price = REALTIME_CACHE['price']
    current_candle_start = int(current_ts // sec) * sec

    if current_candle_start >= last_history_time and current_price > 0:
        if current_candle_start == last_history_time:
            df_res.iloc[-1, df_res.columns.get_loc('close')] = current_price
            df_res.iloc[-1, df_res.columns.get_loc('high')] = max(df_res.iloc[-1]['high'], current_price)
            df_res.iloc[-1, df_res.columns.get_loc('low')] = min(df_res.iloc[-1]['low'], current_price)
        else:
            prev_close = df_res.iloc[-1]['close'] if not df_res.empty else current_price
            new_candle = {
                'timestamp': pd.to_datetime(current_candle_start, unit='s'),
                'open': prev_close,
                'high': max(prev_close, current_price),
                'low': min(prev_close, current_price),
                'close': current_price,
                'time': current_candle_start
            }
            df_res = pd.concat([df_res, pd.DataFrame([new_candle])], ignore_index=True)

    return df_res[['time', 'open', 'high', 'low', 'close']].to_dict(orient='records')

# --- ENDPOINTS ---
@app.get("/")
async def read_index(): return FileResponse('index.html')

@app.get("/api/history/{symbol}")
def get_history(symbol: str, interval: str = '15m'):
    try:
        folder_path = f"s3://bronze/coin_prices/history_{symbol}.parquet"
        init_file = f"{folder_path}/init.parquet"
        read_target = init_file if fs.exists(init_file) else folder_path

        if fs.exists(read_target):
            try:
                df = pd.read_parquet(read_target, filesystem=fs)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                return resample_ohlc(df, interval)
            except Exception as read_err:
                print(f"⚠️ Lỗi đọc file Parquet: {read_err}")
                return []
        else:
            print(f"ℹ️ Không tìm thấy dữ liệu tại: {read_target}")
            return []

    except Exception as e:
        print(f"❌ Lỗi API History: {e}")
        return []

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    if REALTIME_CACHE["price"] == 0:
        return []
    return [{"time": int(REALTIME_CACHE["timestamp"] / 1000), "close": REALTIME_CACHE["price"]}]

class ChatRequest(BaseModel):
    message: str

@app.post("/api/chat")
async def chat_ai(request: ChatRequest):
    price = REALTIME_CACHE['price']
    prompt = f"Giá BTC hiện tại: ${price:,.2f}. Câu hỏi: {request.message}. Trả lời ngắn gọn tiếng Việt dưới 50 từ."

    models_to_try = [
        "gemini-2.5-flash",       # Model mới nhất, mạnh nhất
        "gemini-2.0-flash",       # Bản ổn định
        "gemini-2.0-flash-lite",  # Bản siêu tốc, tiết kiệm
    ]
    response_text = ""
    model_used = "unknown"

    # 1. Gọi AI
    for model_name in models_to_try:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt
            )
            response_text = response.text
            model_used = model_name
            break
        except Exception as e:
            print(f"⚠️ Model {model_name} lỗi: {e}")
            continue

    # Fallback
    if not response_text:
        trend = "tăng" if price > 96000 else "giảm"
        response_text = f"BTC đang {trend} nhẹ quanh mức ${price:,.2f}. (AI đang quá tải, thử lại sau)."
        model_used = "fallback-rule"

    # 2. Lưu Log
    if engine:
        try:
            db = SessionLocal()
            log_entry = ChatLog(
                message=request.message,
                response=response_text,
                model_used=model_used
            )
            db.add(log_entry)
            db.commit()
            db.close()
            print(f"💾 Đã lưu log chat [User: {request.message}]")
        except Exception as e:
            print(f"❌ Lỗi lưu DB: {e}")

    return {"response": response_text}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)