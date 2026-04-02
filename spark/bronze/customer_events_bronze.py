from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path

# Initialize Spark
spark = get_spark("customer_events_bronze")

# Schema for customer events
customer_events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("device_type", StringType()),
    StructField("region", StringType())
])

# Base path (hybrid: S3 or local)
BASE_PATH = bronze_path() + "customer_events"
CHECKPOINT_PATH = BASE_PATH + "/_checkpoint"

# Read from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customer_events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON
parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), customer_events_schema).alias("data"))
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