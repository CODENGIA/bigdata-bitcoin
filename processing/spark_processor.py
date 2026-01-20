import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- CẤU HÌNH ĐỘNG (Lấy từ K8s env hoặc mặc định) ---
# Trong K8s, Kafka Service tên là 'kafka', cổng nội bộ là 9092
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
# Trong K8s, MinIO Service tên là 'minio', cổng api là 9000
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')

ACCESS_KEY = "admin"
SECRET_KEY = "password123"

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

    # Schema dữ liệu từ Kafka (Khớp với Producer)
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

    # Chuyển đổi event_time (ms) sang timestamp chuẩn
    processed_df = parsed_df.withColumn("timestamp", (col("event_time") / 1000).cast("timestamp"))

    print("--- Writing to MinIO (Bronze Layer - Parquet) ---")

    # 4. Ghi dữ liệu vào MinIO dưới dạng Parquet
    # Lưu ý: Dùng Parquet thay vì Delta để tránh lỗi thiếu JAR trong K8s
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/coin_data_parquet") \
        .option("path", "s3a://bronze/coin_prices/history_BTCUSDT.parquet") \
        .trigger(processingTime='10 seconds') \
        .start()

    print("--- Streaming is running... Waiting for data ---")
    query.awaitTermination()

if __name__ == "__main__":
    main()