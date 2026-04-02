from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path, silver_path

spark = get_spark("orders_silver")

# Paths
BRONZE_PATH = bronze_path() + "orders"
SILVER_PATH = silver_path() + "orders"
CHECKPOINT_PATH = SILVER_PATH + "/_checkpoint"

# Read Bronze as streaming Delta
bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

# Transformations
silver_df = (
    bronze_df
    # Convert timestamp
    .withColumn("order_ts", to_timestamp(col("order_timestamp")))
    # Normalize currency
    .withColumn("currency", upper(col("currency")))
    # Add ingestion timestamp
    .withColumn("processed_ts", current_timestamp())
    # Deduplicate
    .withWatermark("order_ts", "10 minutes")
    .dropDuplicates(["order_id"])
)

# Write to Silver
query = (
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start(SILVER_PATH)
)

query.awaitTermination()