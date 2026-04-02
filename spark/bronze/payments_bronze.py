from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path

# Initialize Spark
spark = get_spark("payments_bronze")

# Schema for payment events
payments_schema = StructType([
    StructField("payment_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("payment_status", StringType()),
    StructField("payment_timestamp", StringType()),
    StructField("region", StringType())
])

# Base path (hybrid: S3 or local)
BASE_PATH = bronze_path() + "payments"
CHECKPOINT_PATH = BASE_PATH + "/_checkpoint"

# Read from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "payments")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON
parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), payments_schema).alias("data"))
      .select("data.*")
      .withColumn("ingest_ts", current_timestamp())
)

# Write to Delta (Hybrid: S3 or Local)
query = (
    parsed_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start(BASE_PATH)
)

query.awaitTermination()