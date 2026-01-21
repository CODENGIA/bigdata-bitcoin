import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- CẤU HÌNH ĐỘNG ---
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')

# Credentials (trong môi trường thật nên dùng Secret, ở đây demo hardcode hoặc env)
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

def main():
    print(f"--- SPARK CONFIG ---")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"MinIO: {MINIO_ENDPOINT}")

    # 1. Khởi tạo Spark Session với cấu hình S3 (MinIO)
    spark = SparkSession.builder \
        .appName("CryptoStreamProcessor") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schema dữ liệu từ Kafka
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("event_time", LongType(), True)
    ])

    print("--- Connecting to Kafka ---")

    # 2. Đọc luồng từ Kafka
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("subscribe", "coin-ticker") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    except Exception as e:
        print(f"CRITICAL ERROR: Không thể kết nối Kafka tại {KAFKA_BOOTSTRAP}. Lỗi: {e}")
        sys.exit(1)

    # 3. Parse dữ liệu JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Chuyển đổi event_time sang timestamp
    processed_df = parsed_df.withColumn("timestamp", (col("event_time") / 1000).cast("timestamp"))

    print("--- Writing to MinIO (Bronze Layer - Parquet) ---")

    # 4. Ghi dữ liệu (Phân luồng theo Symbol)
    
    # --- LUỒNG 1: BTCUSDT ---
    query_btc = processed_df.filter("symbol = 'BTCUSDT'") \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/btc") \
        .option("path", "s3a://bronze/coin_prices/history_BTCUSDT.parquet") \
        .trigger(processingTime='10 seconds') \
        .start()

    # --- LUỒNG 2: ETHUSDT ---
    query_eth = processed_df.filter("symbol = 'ETHUSDT'") \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/eth") \
        .option("path", "s3a://bronze/coin_prices/history_ETHUSDT.parquet") \
        .trigger(processingTime='10 seconds') \
        .start()

    print(">>> Spark Streaming đang chạy song song cho BTC và ETH...")
    
    # Đợi bất kỳ luồng nào kết thúc (hoặc lỗi)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()