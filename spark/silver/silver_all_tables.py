from pyspark.sql.functions import col
from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path, silver_path

from pyspark.sql.functions import (
    to_timestamp, current_timestamp, upper
)

spark = get_spark("silver_all_tables")

# -----------------------------
# Helper: STREAMING SILVER JOB
# -----------------------------
def start_streaming_silver(
    bronze_subpath: str,
    silver_subpath: str,
    timestamp_col: str,
    dedupe_key: str,
    watermark="10 minutes"
):
    bronze = bronze_path() + bronze_subpath
    silver = silver_path() + silver_subpath
    checkpoint = silver + "/_checkpoint"

    df = (
        spark.readStream
        .format("delta")
        .load(bronze)
    )

    transformed = (
        df
        .withColumn(timestamp_col, to_timestamp(col(timestamp_col)))
        .withColumn("processed_ts", current_timestamp())
        .withWatermark(timestamp_col, watermark)
        .dropDuplicates([dedupe_key])
    )

    query = (
        transformed.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start(silver)
    )

    return query


# -----------------------------
# Helper: BATCH SILVER JOB
# -----------------------------
def run_batch_silver(
    bronze_subpath: str,
    silver_subpath: str,
    timestamp_col: str,
    dedupe_key: str
):
    bronze = bronze_path() + bronze_subpath
    silver = silver_path() + silver_subpath

    df = (
        spark.read
        .format("delta")
        .load(bronze)
    )

    transformed = (
        df
        .withColumn(timestamp_col, to_timestamp(col(timestamp_col)))
        .withColumn("processed_ts", current_timestamp())
        .dropDuplicates([dedupe_key])
    )

    (
        transformed.write
        .format("delta")
        .mode("overwrite")
        .save(silver)
    )


# -----------------------------
# Run BATCH Silver first
# -----------------------------
print("Running batch Silver tables...")

run_batch_silver(
    bronze_subpath="customer_events",
    silver_subpath="customer_events",
    timestamp_col="event_timestamp",
    dedupe_key="event_id"
)

run_batch_silver(
    bronze_subpath="inventory",
    silver_subpath="inventory",
    timestamp_col="last_updated",
    dedupe_key="inventory_id"
)

print("Batch Silver complete.")


# -----------------------------
# Start STREAMING Silver
# -----------------------------
print("Starting streaming Silver tables...")

queries = []

queries.append(
    start_streaming_silver(
        bronze_subpath="orders",
        silver_subpath="orders",
        timestamp_col="order_timestamp",
        dedupe_key="order_id"
    )
)

queries.append(
    start_streaming_silver(
        bronze_subpath="payments",
        silver_subpath="payments",
        timestamp_col="payment_timestamp",
        dedupe_key="payment_id"
    )
)

queries.append(
    start_streaming_silver(
        bronze_subpath="shipments",
        silver_subpath="shipments",
        timestamp_col="shipment_timestamp",
        dedupe_key="shipment_id",
        watermark="30 minutes"
    )
)

print("Streaming Silver jobs started.")

# Keep all streaming queries alive
spark.streams.awaitAnyTermination()