import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg,
    coalesce,
    lit,
    current_timestamp
)

# ----------------------------------------------------
# FIX PYTHON PATH FOR PROJECT ROOT
# ----------------------------------------------------
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
)

from spark.utils.paths import silver_path, gold_path

# ----------------------------------------------------
# DELTA‑ENABLED SPARK SESSION
# ----------------------------------------------------
spark = (
    SparkSession.builder
    .appName("inventory_replenishment_gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ----------------------------------------------------
# PATHS
# ----------------------------------------------------
SILVER_INVENTORY = silver_path() + "inventory/"
SILVER_ORDERS = silver_path() + "orders/"

GOLD_INVENTORY = gold_path() + "inventory_replenishment/"

# ----------------------------------------------------
# LOAD SILVER TABLES
# ----------------------------------------------------
inventory_df = spark.read.format("delta").load(SILVER_INVENTORY)
orders_df = spark.read.format("delta").load(SILVER_ORDERS)

# ----------------------------------------------------
# ALIGN INVENTORY SCHEMA
# inventory has item_id, but consumption uses sku
# ----------------------------------------------------
inventory_df = inventory_df.withColumnRenamed("item_id", "sku")

# ----------------------------------------------------
# DAILY CONSUMPTION (orders per item)
# ----------------------------------------------------
daily_consumption = (
    orders_df
    .groupBy("item_id", "region")
    .agg(
        spark_sum("quantity").alias("daily_quantity")
    )
)

# ----------------------------------------------------
# AVERAGE DAILY CONSUMPTION
# ----------------------------------------------------
avg_consumption = (
    daily_consumption
    .groupBy("item_id", "region")
    .agg(
        avg("daily_quantity").alias("avg_daily_consumption")
    )
)

# Rename item_id → sku to match inventory
avg_consumption = avg_consumption.withColumnRenamed("item_id", "sku")

# ----------------------------------------------------
# JOIN INVENTORY WITH CONSUMPTION
# ----------------------------------------------------
inventory = (
    inventory_df.alias("inv")
    .join(
        avg_consumption.alias("c"),
        (col("inv.sku") == col("c.sku")) &
        (col("inv.region") == col("c.region")),
        "left"
    )
    # DROP DUPLICATE COLUMNS FROM THE RIGHT SIDE
    .drop("c.sku", "c.region")
)

# ----------------------------------------------------
# FIX NULLS USING COALESCE
# ----------------------------------------------------
inventory = inventory.withColumn(
    "avg_daily_consumption",
    coalesce(col("avg_daily_consumption"), lit(0))
)

# ----------------------------------------------------
# CALCULATE SAFETY STOCK & REORDER POINT
# ----------------------------------------------------
inventory = (
    inventory
    .withColumn("safety_stock", col("avg_daily_consumption") * lit(3))
    .withColumn("reorder_point", col("avg_daily_consumption") * lit(5))
)

# ----------------------------------------------------
# DETERMINE IF REPLENISHMENT IS NEEDED
# ----------------------------------------------------
inventory = inventory.withColumn(
    "needs_replenishment",
    (col("quantity_available") <= col("reorder_point")).cast("boolean")
)

# ----------------------------------------------------
# FORCE CLEAN FINAL SCHEMA (NO DUPLICATES POSSIBLE)
# ----------------------------------------------------
# FORCE CLEAN FINAL SCHEMA
inventory = inventory.select(
    col("inv.inventory_id").alias("inventory_id"),
    col("inv.sku").alias("sku"),
    col("inv.warehouse_id").alias("warehouse_id"),
    col("inv.quantity_available").alias("quantity_available"),
    col("inv.quantity_reserved").alias("quantity_reserved"),
    col("inv.region").alias("region"),
    "avg_daily_consumption",
    "safety_stock",
    "reorder_point",
    "needs_replenishment",
    "ingest_ts",
    "processed_ts"
)
# ----------------------------------------------------
# ADD PROCESSED TIMESTAMP
# ----------------------------------------------------
inventory = inventory.withColumn("processed_ts", current_timestamp())

# ----------------------------------------------------
# WRITE GOLD TABLE
# ----------------------------------------------------
(
    inventory.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_INVENTORY)
)

spark.stop()