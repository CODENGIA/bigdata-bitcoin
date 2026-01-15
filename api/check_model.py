# FILE: check_model.py
from google import genai

# Key của bạn
client = genai.Client(api_key="AIzaSyDBGkUOid-L9im5hBkvbI-XAVAKGHokC3c")

print("--- DANH SACH MODEL ---")
try:
    for m in client.models.list():
        print(f"ID: {m.name}")
except Exception as e:
    print(f"Loi: {e}")