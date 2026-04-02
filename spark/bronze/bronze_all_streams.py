from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path

spark = get_spark("bronze_all_streams")

# ---------- Schemas ----------

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("item_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("currency", StringType()),
    StructField("order_timestamp", StringType()),
    StructField("region", StringType()),
    StructField("device_type", StringType())
])

customer_events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("device_type", StringType()),
    StructField("region", StringType())
])

inventory_schema = StructType([
    StructField("inventory_id", StringType()),
    StructField("item_id", StringType()),
    StructField("warehouse_id", StringType()),
    StructField("quantity_available", IntegerType()),
    StructField("quantity_reserved", IntegerType()),
    StructField("last_updated", StringType()),
    StructField("region", StringType())
])

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

shipments_schema = StructType([
    StructField("shipment_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("item_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("carrier", StringType()),
    StructField("shipment_status", StringType()),
    StructField("shipment_timestamp", StringType()),
    StructField("region", StringType())
])

# ---------- Helper ----------

def start_stream(topic: str, schema: StructType, table_name: str):
    base_path = bronze_path() + table_name
    checkpoint_path = base_path + "/_checkpoint"

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col("value"), schema).alias("data"))
          .select("data.*")
          .withColumn("ingest_ts", current_timestamp())
    )

    query = (
        parsed_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(base_path)
    )

    return query

# ---------- Start all Bronze streams ----------

queries = [
    start_stream("orders", orders_schema, "orders"),
    start_stream("customer_events", customer_events_schema, "customer_events"),
    start_stream("inventory", inventory_schema, "inventory"),
    start_stream("payments", payments_schema, "payments"),
    start_stream("shipments", shipments_schema, "shipments"),
]

# Wait for any stream to terminate (others keep running until process exits)
spark.streams.awaitAnyTermination()