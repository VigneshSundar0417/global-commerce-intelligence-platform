from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path, silver_path

spark = get_spark("customer_events_silver")

# Paths
BRONZE_PATH = bronze_path() + "customer_events"
SILVER_PATH = silver_path() + "customer_events"

# Read Bronze as batch
bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
)

# Transformations
silver_df = (
    bronze_df
    .withColumn("event_ts", to_timestamp(col("event_timestamp")))
    .withColumn("event_type", upper(col("event_type")))
    .withColumn("device_type", upper(col("device_type")))
    .withColumn("processed_ts", current_timestamp())
    .dropDuplicates(["event_id"])
)

# Write to Silver (overwrite is safe for batch)
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)