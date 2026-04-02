import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    when,
    count,
    sum,
    avg
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = (
    SparkSession.builder
    .appName("order_fulfillment_gold")
    .getOrCreate()
)

# -----------------------------
# LOAD SILVER TABLES
# -----------------------------
orders_df = (
    spark.read.format("delta")
    .load(silver_path() + "orders")
)

payments_df = (
    spark.read.format("delta")
    .load(silver_path() + "payments")
)

shipments_df = (
    spark.read.format("delta")
    .load(silver_path() + "shipments")
)

inventory_df = (
    spark.read.format("delta")
    .load(silver_path() + "inventory")
)

# -----------------------------
# RENAME REGION COLUMNS TO AVOID DUPLICATES
# -----------------------------
orders_df = orders_df.withColumnRenamed("region", "order_region")
payments_df = payments_df.withColumnRenamed("region", "payment_region")
shipments_df = shipments_df.withColumnRenamed("region", "shipment_region")
inventory_df = inventory_df.withColumnRenamed("region", "inventory_region")

# -----------------------------
# FIX TIMESTAMP COLUMNS
# -----------------------------
orders_df = orders_df.withColumn("order_ts", to_timestamp(col("order_timestamp")))
payments_df = payments_df.withColumn("payment_ts", to_timestamp(col("payment_timestamp")))
shipments_df = shipments_df.withColumn("shipment_ts", to_timestamp(col("shipment_timestamp")))
inventory_df = inventory_df.withColumn("last_updated_ts", to_timestamp(col("last_updated")))

# -----------------------------
# SELECT ONLY NECESSARY COLUMNS
# -----------------------------

# Compute total amount (price * quantity)
orders_df = orders_df.withColumn(
    "total_amount",
    col("price") * col("quantity")
)

# Select final columns
orders_df = orders_df.select(
    "order_id",
    "customer_id",
    "order_ts",
    "order_region",
    "total_amount"
)


payments_df = payments_df.select(
    "order_id",
    "payment_ts",
    "amount",
    "payment_method",
    "payment_status",
    "payment_region"
)

shipments_df = shipments_df.select(
    "order_id",
    "shipment_ts",
    "shipment_status",
    "carrier",
    "shipment_region"
)

inventory_df = inventory_df.select(
    "item_id",
    "warehouse_id",
    "quantity_available",
    "quantity_reserved",
    "last_updated_ts",
    "inventory_region"
)

# -----------------------------
# JOIN TABLES
# -----------------------------
order_payment = (
    orders_df.join(payments_df, "order_id", "left")
)

order_payment_ship = (
    order_payment.join(shipments_df, "order_id", "left")
)

# -----------------------------
# FINAL DF (DROP DUPLICATE REGIONS)
# -----------------------------
final_df = (
    order_payment_ship
    .drop("payment_region", "shipment_region")  # keep only order_region
)

# -----------------------------
# WRITE GOLD TABLE
# -----------------------------
(
    final_df.write.format("delta")
    .mode("overwrite")
    .save(gold_path() + "order_fulfillment")
)

spark.stop()