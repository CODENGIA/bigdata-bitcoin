import json
import time
import threading
import os
import pandas as pd
import s3fs
from fastapi import FastAPI, Depends
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from pydantic import BaseModel
from google import genai

# --- IMPORT DATABASE ---
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, func, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

app = FastAPI()

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "DUMMY_KEY")
client = genai.Client(api_key=GOOGLE_API_KEY)

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB = os.getenv("PG_DB")
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL and PG_USER and PG_PASS and PG_DB:
    DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@postgres:5432/{PG_DB}"

# ==============================================================================
# 2. DATABASE & MINIO SETUP
# ==============================================================================
engine = None
SessionLocal = None
Base = declarative_base()

class ChatLog(Base):
    __tablename__ = "chat_logs"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, default="anonymous")
    symbol_context = Column(String)
    message = Column(Text)
    response = Column(Text)
    model_used = Column(String)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

class DailyStat(Base):
    __tablename__ = "daily_stats"
    date = Column(String, primary_key=True) 
    symbol = Column(String, primary_key=True)
    high_price = Column(Float)
    low_price = Column(Float)
    avg_price = Column(Float)
    volatility = Column(Float)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base.metadata.create_all(bind=engine)
        print("✅ DATABASE: Connected!")
    except Exception as e:
        print(f"⚠️ DATABASE ERROR: {e}")

def get_db():
    if SessionLocal:
        db = SessionLocal()
        try: yield db
        finally: db.close()
    else: yield None

# Kết nối MinIO (Global)
try:
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={'endpoint_url': MINIO_ENDPOINT}
    )
except Exception as e:
    print(f"⚠️ MINIO ERROR: {e}")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ==============================================================================
# 3. KAFKA LISTENER
# ==============================================================================
REALTIME_CACHE = {}

def kafka_listener():
    while True:
        try:
            consumer = KafkaConsumer(
                'coin-ticker',
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("✅ KAFKA: Connected!")
            for message in consumer:
                data = message.value
                symbol = data.get('symbol')
                if symbol:
                    REALTIME_CACHE[symbol] = {
                        "price": data["price"],
                        "timestamp": data["event_time"]
                    }
        except Exception as e:
            print(f"⚠️ KAFKA ERROR: {e}. Retrying...")
            time.sleep(5)

threading.Thread(target=kafka_listener, daemon=True).start()

# ==============================================================================
# 4. LOGIC XỬ LÝ
# ==============================================================================
def resample_ohlc(df, interval, symbol):
    interval_seconds = {'1m': 60, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
    sec = interval_seconds.get(interval, 900)
    mapping = {'1m': '1min', '15m': '15min', '1h': '1h', '4h': '4h', '1d': '1D'}
    rule = mapping.get(interval, '15min')

    if 'timestamp' in df.columns:
        df = df.set_index('timestamp')

    agg = {'price': [('open', 'first'), ('high', 'max'), ('low', 'min'), ('close', 'last')]}
    df_res = df.resample(rule).agg(agg)
    df_res.columns = df_res.columns.droplevel(0)
    
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

    if symbol in REALTIME_CACHE:
        cache = REALTIME_CACHE[symbol]
        current_ts = cache['timestamp'] / 1000
        current_price = cache['price']
        
        last_history_time = df_res['time'].iloc[-1] if not df_res.empty else 0
        current_candle_start = int(current_ts // sec) * sec

        if current_candle_start >= last_history_time and current_price > 0:
            if current_candle_start == last_history_time:
                idx = df_res.index[-1]
                df_res.at[idx, 'close'] = current_price
                df_res.at[idx, 'high'] = max(df_res.at[idx, 'high'], current_price)
                df_res.at[idx, 'low'] = min(df_res.at[idx, 'low'], current_price)
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

# ==============================================================================
# 5. ENDPOINTS (FIXED)
# ==============================================================================
@app.get("/")
async def read_index(): return FileResponse('index.html')

@app.get("/api/history/{symbol}")
def get_history(symbol: str, interval: str = '15m'):
    """
    CÁCH FIX: Dùng fs.glob lấy danh sách file -> Đọc từng file -> Gộp lại.
    Cách này KHÔNG bao giờ bị lỗi path.
    """
    try:
        # Đường dẫn folder (không có s3://)
        folder_path = f"bronze/coin_prices/history_{symbol}.parquet"
        
        # 1. Liệt kê tất cả file parquet con
        # (Spark ghi ra part-0000.parquet, part-0001.parquet...)
        files = fs.glob(f"{folder_path}/*.parquet")
        
        if not files:
            # Fallback: check xem có phải file đơn (do Ingestion tạo lúc đầu) không
            if fs.exists(folder_path) and not fs.isdir(folder_path):
                files = [folder_path]
            else:
                return []

        # 2. Đọc từng file
        dfs = []
        for f_path in files:
            try:
                with fs.open(f_path, 'rb') as f:
                    dfs.append(pd.read_parquet(f))
            except Exception as e:
                pass # Bỏ qua file lỗi lẻ tẻ
        
        if not dfs: return []

        # 3. Gộp lại
        df = pd.concat(dfs, ignore_index=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        return resample_ohlc(df, interval, symbol)

    except Exception as e:
        print(f"❌ API Error: {e}")
        return []

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    data = REALTIME_CACHE.get(symbol)
    if not data: return []
    return [{"time": int(data["timestamp"] / 1000), "close": data["price"]}]

@app.get("/api/stats/{symbol}")
def get_stats(symbol: str, db: Session = Depends(get_db)):
    if not db: return {}
    try:
        stat = db.query(DailyStat).filter(DailyStat.symbol == symbol).order_by(desc(DailyStat.updated_at)).first()
        if stat:
            return {
                "high": stat.high_price, "low": stat.low_price,
                "avg": stat.avg_price, "volatility": stat.volatility
            }
    except: pass
    return {}

class ChatRequest(BaseModel):
    message: str
    symbol: str = "BTCUSDT"

@app.post("/api/chat")
async def chat_ai(request: ChatRequest, db: Session = Depends(get_db)):
    cache = REALTIME_CACHE.get(request.symbol, {"price": 0})
    price = cache["price"]
    
    prompt = f"Giá {request.symbol}: ${price:,.2f}. User hỏi: {request.message}. Trả lời ngắn gọn."
    try:
        res = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        text = res.text
    except:
        text = "AI Error"
        
    if db:
        try:
            db.add(ChatLog(message=request.message, response=text, model_used="gemini", symbol_context=request.symbol))
            db.commit()
        except: pass
    return {"response": text}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)