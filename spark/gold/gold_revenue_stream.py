from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, window, to_timestamp

# ---------------------------------------------------------
# 1. SparkSession
# ---------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("GoldRevenueStream")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# 2. Read Silver Orders
# ---------------------------------------------------------
silver_orders_path = "/home/vicky/global-commerce-intelligence-platform/data/silver/orders"

orders_df = (
    spark.readStream
    .format("delta")
    .load(silver_orders_path)
)

# Convert timestamp
orders_df = orders_df.withColumn(
    "order_ts",
    to_timestamp(col("order_timestamp"))
)

# Compute revenue
orders_df = orders_df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

# ---------------------------------------------------------
# 3. Aggregate Revenue (Hourly)
# ---------------------------------------------------------
revenue_agg = (
    orders_df
    .groupBy(
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
# 4. Write to Gold Delta Table
# ---------------------------------------------------------
gold_path = "/home/vicky/global-commerce-intelligence-platform/data/gold/revenue"

query = (
    revenue_agg.writeStream
    .format("delta")
    .option("checkpointLocation", f"{gold_path}/_checkpoint")
    .outputMode("complete")
    .start(gold_path)
)

print("🚀 Gold Revenue Stream is running...")

query.awaitTermination()