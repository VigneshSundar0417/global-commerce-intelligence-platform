import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    sum as spark_sum,
    to_date,
    to_timestamp,
    window,
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
    .appName("conversion_funnel_gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ----------------------------------------------------
# PATHS
# ----------------------------------------------------
SILVER_EVENTS = silver_path() + "customer_events/"
SILVER_ORDERS = silver_path() + "orders/"
SILVER_PAYMENTS = silver_path() + "payments/"

GOLD_FUNNEL = gold_path() + "conversion_funnel/"

# ----------------------------------------------------
# LOAD SILVER TABLES
# ----------------------------------------------------
events_df = spark.read.format("delta").load(SILVER_EVENTS)
orders_df = spark.read.format("delta").load(SILVER_ORDERS)
payments_df = spark.read.format("delta").load(SILVER_PAYMENTS)

# ----------------------------------------------------
# PREPARE EVENTS STAGE
# ----------------------------------------------------
funnel_events = (
    events_df
    .withColumn("event_date", to_date(col("event_timestamp")))
    .groupBy("event_date", "region", "device_type")
    .agg(
        count(when(col("event_type") == "VIEW", True)).alias("views"),
        count(when(col("event_type") == "CLICK", True)).alias("clicks"),
        count(when(col("event_type") == "ADD_TO_CART", True)).alias("add_to_cart")
    )
)

# ----------------------------------------------------
# PREPARE ORDERS STAGE
# ----------------------------------------------------
orders_stage = (
    orders_df
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "region", "device_type")
    .agg(
        count("order_id").alias("orders")
    )
)

# ----------------------------------------------------
# PREPARE PAYMENTS STAGE
# ----------------------------------------------------
payments_stage = (
    payments_df
    .withColumn("payment_date", to_date(col("payment_timestamp")))
    .groupBy("payment_date", "region")
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("order_id").alias("successful_payments")
    )
)

# ----------------------------------------------------
# JOIN STAGES — NO DUPLICATE COLUMNS
# ----------------------------------------------------
funnel = (
    funnel_events.alias("fe")
    .join(
        orders_stage.alias("o"),
        (col("fe.event_date") == col("o.order_date")) &
        (col("fe.region") == col("o.region")) &
        (col("fe.device_type") == col("o.device_type")),
        "left"
    )
    .join(
        payments_stage.alias("p"),
        (col("fe.event_date") == col("p.payment_date")) &
        (col("fe.region") == col("p.region")),
        "left"
    )
    # DROP ALL DUPLICATES EXPLICITLY
    .drop("o.region", "o.device_type", "o.order_date")
    .drop("p.region", "p.payment_date")
    .select(
        "fe.event_date",
        "fe.region",
        "fe.device_type",
        "views",
        "clicks",
        "add_to_cart",
        "orders",
        "total_revenue",
        "successful_payments",
    )
    .fillna(0)
    .withColumn("processed_ts", current_timestamp())
)

# ----------------------------------------------------
# WRITE GOLD TABLE
# ----------------------------------------------------
(
    funnel.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_FUNNEL)
)

spark.stop()