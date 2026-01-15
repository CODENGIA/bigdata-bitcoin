import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Cấu hình MinIO
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"

def main():
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("CryptoStreamProcessor") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
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
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "coin-ticker") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 100) \
        .load()

    # 3. Parse dữ liệu JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Chuyển đổi event_time sang timestamp
    parsed_df = parsed_df.withColumn("timestamp", (col("event_time") / 1000).cast("timestamp"))

    print("--- Writing to MinIO (Bronze Layer) ---")

    # 4. Ghi dữ liệu vào Delta Table
    # Đảm bảo các dòng .option được thụt lề bằng nhau (4 hoặc 8 dấu cách)
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/coin_data_v5") \
        .option("path", "s3a://bronze/coin_prices") \
        .trigger(processingTime='15 seconds') \
        .start()

    print("--- Streaming is running ---")
    query.awaitTermination()

if __name__ == "__main__":
    main()