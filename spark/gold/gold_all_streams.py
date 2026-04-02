from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, window, to_timestamp
)

# ---------------------------------------------------------
# 1. SparkSession
# ---------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("GoldAllStreams")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

base = "/home/vicky/global-commerce-intelligence-platform/data"

# ---------------------------------------------------------
# Helper: Start a streaming write
# ---------------------------------------------------------
def start_stream(df, output_path, name):
    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{output_path}/_checkpoint")
        .outputMode("complete")
        .start(output_path)
    )
    print(f"🚀 Started Gold stream: {name}")
    return query

# ---------------------------------------------------------
# 2. Revenue Gold
# ---------------------------------------------------------
orders = (
    spark.readStream
    .format("delta")
    .load(f"{base}/silver/orders")
)

orders = orders.withColumn("order_ts", to_timestamp(col("order_timestamp")))
orders = orders.withColumn("revenue", col("quantity") * col("price"))

revenue_agg = (
    orders.groupBy(
        window(col("order_ts"), "1 hour").alias("hour_window"),
        col("region")
    )
    .agg(
        _sum("revenue").alias("total_revenue"),
        _sum("quantity").alias("total_items_sold")
    )
    .select(
        col("hour_window.start").alias("hour_start"),
        col("hour_window.end").alias("hour_end"),
        "region",
        "total_revenue",
        "total_items_sold"
    )
)

# ---------------------------------------------------------
# 3. Payment Success Rate Gold
# ---------------------------------------------------------
payments = (
    spark.readStream
    .format("delta")
    .load(f"{base}/silver/payments")
)

payments = payments.withColumn("payment_ts", to_timestamp(col("payment_timestamp")))

payment_agg = (
    payments.groupBy(
        window(col("payment_ts"), "1 hour").alias("hour_window"),
        col("region")
    )
    .agg(
        count("*").alias("total_payments"),
        _sum((col("payment_status") == "SUCCESS").cast("int")).alias("successful_payments"),
        _sum((col("payment_status") == "FAILED").cast("int")).alias("failed_payments"),
        _sum("amount").alias("total_amount")
    )
    .select(
        col("hour_window.start").alias("hour_start"),
        col("hour_window.end").alias("hour_end"),
        "region",
        "total_payments",
        "successful_payments",
        "failed_payments",
        (col("successful_payments") / col("total_payments")).alias("success_rate"),
        "total_amount"
    )
)

# ---------------------------------------------------------
# 4. Inventory Health Gold
# ---------------------------------------------------------
inventory = (
    spark.readStream
    .format("delta")
    .load(f"{base}/silver/inventory")
)

inventory_agg = (
    inventory.groupBy("item_id", "region")
    .agg(
        _sum("available_qty").alias("total_available"),
        _sum("reserved_qty").alias("total_reserved")
    )
)

# ---------------------------------------------------------
# 5. Shipment Performance Gold
# ---------------------------------------------------------
shipments = (
    spark.readStream
    .format("delta")
    .load(f"{base}/silver/shipments")
)

shipments = shipments.withColumn("shipped_ts", to_timestamp(col("shipped_timestamp")))
shipments = shipments.withColumn("delivered_ts", to_timestamp(col("delivered_timestamp")))

shipment_agg = (
    shipments.groupBy(
        window(col("shipped_ts"), "1 hour").alias("hour_window"),
        col("region")
    )
    .agg(
        count("*").alias("total_shipments"),
        _sum((col("status") == "DELIVERED").cast("int")).alias("delivered_count"),
        _sum((col("status") == "IN_TRANSIT").cast("int")).alias("in_transit_count")
    )
    .select(
        col("hour_window.start").alias("hour_start"),
        col("hour_window.end").alias("hour_end"),
        "region",
        "total_shipments",
        "delivered_count",
        "in_transit_count"
    )
)

# ---------------------------------------------------------
# 6. Customer Engagement Gold
# ---------------------------------------------------------
events = (
    spark.readStream
    .format("delta")
    .load(f"{base}/silver/customer_events")
)

events = events.withColumn("event_ts", to_timestamp(col("event_timestamp")))

engagement_agg = (
    events.groupBy(
        window(col("event_ts"), "1 hour").alias("hour_window"),
        col("region"),
        col("event_type")
    )
    .agg(
        count("*").alias("event_count")
    )
    .select(
        col("hour_window.start").alias("hour_start"),
        col("hour_window.end").alias("hour_end"),
        "region",
        "event_type",
        "event_count"
    )
)

# ---------------------------------------------------------
# 7. Start all Gold streams
# ---------------------------------------------------------
queries = []

queries.append(start_stream(revenue_agg, f"{base}/gold/revenue", "revenue"))
queries.append(start_stream(payment_agg, f"{base}/gold/payment_success_rate", "payment_success_rate"))
queries.append(start_stream(inventory_agg, f"{base}/gold/inventory_health", "inventory_health"))
queries.append(start_stream(shipment_agg, f"{base}/gold/shipment_performance", "shipment_performance"))
queries.append(start_stream(engagement_agg, f"{base}/gold/customer_engagement", "customer_engagement"))

print("🔥 All Gold streams are running inside ONE SparkSession")

spark.streams.awaitAnyTermination()