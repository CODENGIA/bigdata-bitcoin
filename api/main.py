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

# --- [CAU HINH API KEY] ---
GOOGLE_API_KEY = "AIzaSyDBGkUOid-L9im5hBkvbI-XAVAKGHokC3c"
client = genai.Client(api_key=GOOGLE_API_KEY)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# --- HA TANG ---
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
            print("✅ API: Da ket noi Kafka!")
            for message in consumer:
                data = message.value
                REALTIME_CACHE["price"] = data["price"]
                REALTIME_CACHE["timestamp"] = data["event_time"]
        except:
            time.sleep(5)

threading.Thread(target=kafka_listener, daemon=True).start()

# --- ENDPOINTS ---
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
            mapping = {'1m': '1min', '15m': '15min', '1h': '1h', '4h': '4h', '1d': '1D'}
            rule = mapping.get(interval, '15min')
            df = df.set_index('timestamp')
            df_res = df.resample(rule).agg({'price': [('open', 'first'), ('high', 'max'), ('low', 'min'), ('close', 'last')]})
            df_res.columns = df_res.columns.droplevel(0)
            df_res = df_res.dropna().reset_index()
            df_res['time'] = df_res['timestamp'].astype('int64') // 10**9
            return df_res[['time', 'open', 'high', 'low', 'close']].to_dict(orient='records')
    except: return []

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    if REALTIME_CACHE["price"] == 0: return []
    return [{"time": int(REALTIME_CACHE["timestamp"] / 1000), "close": REALTIME_CACHE["price"]}]

class ChatRequest(BaseModel): message: str

@app.post("/api/chat")
async def chat_ai(request: ChatRequest):
    price = REALTIME_CACHE['price']
    try:
        # GOI DUNG MODEL GEMINI 3.0 MA PHONG MUON
        response = client.models.generate_content(
            model="gemini-3-pro-preview", 
            contents=f"Gia BTC: ${price}. Cau hoi: {request.message}. Tra loi ngan gon tieng Viet."
        )
        return {"response": response.text}
    except Exception as e:
        print(f"❌ LOI AI: {str(e)}")
        # Neu 3.0 dang qua tai (vi la ban preview), tu dong chuyen sang 2.5 Flash
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash", 
                contents=f"Gia BTC: ${price}. Cau hoi: {request.message}. Tra loi ngan gon tieng Viet."
            )
            return {"response": response.text}
        except:
            return {"response": f"BTC dang o muc ${price}. AI dang phan tich du lieu."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)