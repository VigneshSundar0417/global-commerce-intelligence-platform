import sys
import os

# Add project root to Python path (3 levels up)
# gold → spark → project root
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    window,
    sum as spark_sum,
    count
)

# Import correct paths module
from spark.utils.paths import silver_path, gold_path

print("DEBUG silver_path() =", silver_path())
# ---------------------------------------
# SPARK SESSION
# ---------------------------------------
spark = (
    SparkSession.builder
    .appName("revenue_gold_hourly")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


# ---------------------------------------
# LOAD SILVER PAYMENTS TABLE
# ---------------------------------------
payments_df = (
    spark.read.format("delta")
    .load(silver_path() + "payments/")   # IMPORTANT: trailing slash
)


# ---------------------------------------
# RENAME CURRENCY TO AVOID DUPLICATES
# ---------------------------------------
payments_df = payments_df.withColumnRenamed("currency", "payment_currency")


# ---------------------------------------
# FIX TIMESTAMP COLUMN
# ---------------------------------------
payments_df = payments_df.withColumn(
    "payment_ts",
    to_timestamp(col("payment_timestamp"))
)


# ---------------------------------------
# FILTER SUCCESSFUL PAYMENTS
# ---------------------------------------
payments_df = payments_df.filter(col("payment_status") == "SUCCESS")


# ---------------------------------------
# SELECT REQUIRED COLUMNS
# ---------------------------------------
payments_df = payments_df.select(
    "order_id",
    "payment_ts",
    "amount",
    "payment_currency"
)


# ---------------------------------------
# ADD DATE + HOURLY WINDOW
# ---------------------------------------
payments_df = payments_df.withColumn(
    "event_date",
    to_date(col("payment_ts"))
)

payments_df = payments_df.withColumn(
    "hour_window",
    window(col("payment_ts"), "1 hour")
)


# ---------------------------------------
# AGGREGATE HOURLY REVENUE
# ---------------------------------------
hourly_revenue_df = (
    payments_df.groupBy("event_date", "hour_window", "payment_currency")
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("order_id").alias("orders_count")
    )
)


# ---------------------------------------
# WRITE GOLD TABLE
# ---------------------------------------
(
    hourly_revenue_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path() + "revenue_hourly/")
)


spark.stop()