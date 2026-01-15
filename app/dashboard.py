# import streamlit as st
# import pandas as pd
# import plotly.graph_objects as go
# import google.generativeai as genai
# import s3fs
# import time

# # --- 1. CẤU HÌNH ---
# st.set_page_config(page_title="Crypto AI Analytics", layout="wide")

# st.markdown("""
# <style>
#     .stApp { background-color: #0e1117; }
#     .main * { color: #ffffff !important; }
#     [data-testid="stSidebar"] * { color: #000000 !important; }
#     [data-testid="stSidebar"] { background-color: #f0f2f6; }
#     .stAlert { background-color: #262730; color: #fff; }
#     .block-container { padding-top: 1rem; }
#     h1 { margin-bottom: 0px; }
#     .stDeployButton {display:none;}
# </style>
# """, unsafe_allow_html=True)

# # --- 2. KẾT NỐI MINIO ---
# try:
#     fs = s3fs.S3FileSystem(
#         key="admin", secret="password123",
#         client_kwargs={'endpoint_url': 'http://localhost:9000'}
#     )
# except: pass

# # --- 3. STATE & CONFIG ---
# if 'messages' not in st.session_state: st.session_state.messages = []
# if 'GOOGLE_API_KEY' not in st.session_state: st.session_state.GOOGLE_API_KEY = ""

# # --- 4. SIDEBAR ---
# with st.sidebar:
#     st.markdown("### 🎛️ Control Panel")
#     api_key = st.text_input("Gemini API Key", type="password")
#     if api_key:
#         st.session_state.GOOGLE_API_KEY = api_key
#         genai.configure(api_key=api_key)
    
#     coin_pair = st.selectbox("Cặp tiền", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    
#     # THANH ĐIỀU KHIỂN ZOOM (Thay cho Lazy Loading thủ công)
#     st.markdown("### 🔍 Zoom Level")
#     zoom_option = st.select_slider(
#         "Chọn phạm vi hiển thị:",
#         options=["Real-time (1H)", "24 Giờ", "7 Ngày", "30 Ngày", "Full History"],
#         value="Real-time (1H)"
#     )
    
#     st.caption("Hệ thống tự động điều chỉnh độ phân giải nến để tối ưu tốc độ.")

# # --- 5. LOGIC DỮ LIỆU THÔNG MINH ---

# # Load toàn bộ lịch sử vào RAM (Chỉ làm 1 lần)
# @st.cache_data(ttl=3600, show_spinner=False)
# def load_full_history(symbol):
#     try:
#         path = f"s3://bronze/coin_prices/history_{symbol}.parquet"
#         if fs.exists(path):
#             with fs.open(path, 'rb') as f:
#                 return pd.read_parquet(f)
#     except: pass
#     return pd.DataFrame()

# # Load dữ liệu mới nhất (Mỗi giây)
# def load_realtime_buffer(symbol):
#     try:
#         files = sorted(fs.glob("s3://bronze/coin_prices/part-*.parquet"), reverse=True)[:5]
#         frames = [pd.read_parquet(fs.open(f, 'rb')) for f in files]
#         if frames:
#             df = pd.concat(frames)
#             return df[df['symbol'] == symbol]
#     except: pass
#     return pd.DataFrame()

# def get_smart_data(symbol, zoom_mode):
#     # 1. Gộp dữ liệu
#     df_hist = load_full_history(symbol)
#     df_rt = load_realtime_buffer(symbol)
    
#     if df_hist.empty and df_rt.empty: return None, "1m"
    
#     df = pd.concat([df_hist, df_rt])
#     df['timestamp'] = pd.to_datetime(df['timestamp'])
#     df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
#     df = df.set_index('timestamp')
    
#     # 2. LOGIC "SMART DOWNSAMPLING" (Bí quyết mượt mà)
#     # Tự động chọn khung nến dựa trên mức Zoom
#     if zoom_mode == "Real-time (1H)":
#         # Chỉ lấy 1 tiếng cuối, giữ nguyên nến 1 phút
#         cutoff = pd.Timestamp.now() - pd.Timedelta(hours=1)
#         sliced_df = df[df.index >= cutoff]
#         final_df = sliced_df.resample('1Min').ohlc().dropna()
#         tf_display = "1 Phút"
        
#     elif zoom_mode == "24 Giờ":
#         # Lấy 24h, resample thành nến 5 phút cho đỡ rối
#         cutoff = pd.Timestamp.now() - pd.Timedelta(hours=24)
#         sliced_df = df[df.index >= cutoff]
#         final_df = sliced_df.resample('5Min').ohlc().dropna()
#         tf_display = "5 Phút"
        
#     elif zoom_mode == "7 Ngày":
#         # Lấy 7 ngày, resample thành nến 30 phút
#         cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
#         sliced_df = df[df.index >= cutoff]
#         final_df = sliced_df.resample('30Min').ohlc().dropna()
#         tf_display = "30 Phút"

#     elif zoom_mode == "30 Ngày":
#         # Lấy 30 ngày, resample thành nến 4 Giờ
#         cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
#         sliced_df = df[df.index >= cutoff]
#         final_df = sliced_df.resample('4h').ohlc().dropna()
#         tf_display = "4 Giờ"
        
#     else: # Full History
#         # Lấy hết, resample thành nến 1 Ngày
#         final_df = df.resample('1D').ohlc().dropna()
#         tf_display = "1 Ngày"
        
#     return final_df, tf_display

# # --- 6. RENDER ---
# @st.fragment(run_every=2) # 2 giây cập nhật 1 lần
# def render_main():
#     df, tf_name = get_smart_data(coin_pair, zoom_option)
    
#     if df is None or df.empty:
#         st.warning("⏳ Đang khởi động Data Pipeline...")
#         return

#     # Lấy giá mới nhất (từ nến 1m gốc để chính xác)
#     # Dù đang xem nến 1 Ngày thì giá hiển thị vẫn phải là giá giây hiện tại
#     last_close = df.iloc[-1]['close']
#     prev_close = df.iloc[-2]['close'] if len(df) > 1 else last_close
#     change = last_close - prev_close
    
#     # Metrics
#     c1, c2, c3 = st.columns(3)
#     c1.metric(f"{coin_pair}", f"${last_close:,.2f}", f"{change:,.2f}")
#     c2.metric("High (View)", f"${df['high'].max():,.2f}")
#     c3.metric(f"Nến: {tf_name}", f"Live Updates")

#     # Chart
#     fig = go.Figure(data=[go.Candlestick(
#         x=df.index,
#         open=df['open'], high=df['high'],
#         low=df['low'], close=df['close'],
#         increasing_line_color='#00ffcc',
#         decreasing_line_color='#ff3366'
#     )])

#     fig.update_layout(
#         template="plotly_dark",
#         height=500, autosize=True,
#         xaxis_rangeslider_visible=False,
#         margin=dict(l=0, r=0, t=10, b=0),
#         uirevision='constant', # Chống giật khi cập nhật
#         yaxis=dict(side='right', gridcolor='#222'),
#         xaxis=dict(gridcolor='#222')
#     )
#     st.plotly_chart(fig, width="stretch")
#     st.session_state['current_price'] = last_close

# # --- 7. UI ---
# st.markdown(f"### 🚀 Crypto AI Lakehouse")
# col_1, col_2 = st.columns([3, 1])

# with col_1:
#     render_main()

# with col_2:
#     st.markdown("#### 🤖 AI Chat")
#     chat_box = st.container(height=450)
#     with chat_box:
#         for msg in st.session_state.messages:
#             st.chat_message(msg["role"]).write(msg["content"])
            
#     if q := st.chat_input("Hỏi AI..."):
#         st.session_state.messages.append({"role": "user", "content": q})
#         chat_box.chat_message("user").write(q)
#         # (AI Logic giữ nguyên...)