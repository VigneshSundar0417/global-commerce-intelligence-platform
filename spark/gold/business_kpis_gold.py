from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, to_date, current_timestamp
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = get_spark("business_kpis_gold")

# Silver sources
SILVER_ORDERS = silver_path() + "orders"
SILVER_PAYMENTS = silver_path() + "payments"
SILVER_SHIPMENTS = silver_path() + "shipments"
SILVER_EVENTS = silver_path() + "customer_events"
SILVER_INVENTORY = silver_path() + "inventory"

# Gold output
GOLD_BUSINESS_KPIS = gold_path() + "business_kpis_daily"

# -----------------------------
# Load Silver tables
# -----------------------------
orders_df = spark.read.format("delta").load(SILVER_ORDERS)
payments_df = spark.read.format("delta").load(SILVER_PAYMENTS)
shipments_df = spark.read.format("delta").load(SILVER_SHIPMENTS)
events_df = spark.read.format("delta").load(SILVER_EVENTS)
inventory_df = spark.read.format("delta").load(SILVER_INVENTORY)

# -----------------------------
# Orders KPIs
# -----------------------------
orders_daily = (
    orders_df
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("orders_count"),
        _sum(col("price") * col("quantity")).alias("gross_revenue")
    )
)

# -----------------------------
# Payments KPIs
# -----------------------------
payments_success = (
    payments_df
    .filter(col("payment_status") == "SUCCESS")
    .withColumn("payment_date", to_date(col("payment_timestamp")))
    .groupBy("payment_date")
    .agg(
        _sum("amount").alias("collected_revenue"),
        countDistinct("payment_id").alias("successful_payments")
    )
)

# -----------------------------
# Shipments KPIs
# -----------------------------
shipments_daily = (
    shipments_df
    .withColumn("ship_date", to_date(col("shipment_timestamp")))
    .groupBy("ship_date")
    .agg(
        countDistinct("shipment_id").alias("shipments_count")
    )
)

# -----------------------------
# Customer Engagement KPIs
# -----------------------------
events_daily = (
    events_df
    .withColumn("event_date", to_date(col("event_timestamp")))
    .groupBy("event_date")
    .agg(
        countDistinct("customer_id").alias("active_customers"),
        countDistinct("event_id").alias("total_events")
    )
)

# -----------------------------
# Inventory KPIs
# -----------------------------
inventory_daily = (
    inventory_df
    .withColumn("inventory_date", to_date(col("last_updated")))
    .groupBy("inventory_date")
    .agg(
        _sum("quantity_available").alias("total_available_inventory")
    )
)

# -----------------------------
# Join all KPIs into one table
# -----------------------------
kpis = (
    orders_daily
    .join(payments_success, orders_daily.order_date == payments_success.payment_date, "left")
    .join(shipments_daily, orders_daily.order_date == shipments_daily.ship_date, "left")
    .join(events_daily, orders_daily.order_date == events_daily.event_date, "left")
    .join(inventory_daily, orders_daily.order_date == inventory_daily.inventory_date, "left")
    .select(
        orders_daily.order_date.alias("date"),
        "orders_count",
        "gross_revenue",
        "collected_revenue",
        "successful_payments",
        "shipments_count",
        "active_customers",
        "total_events",
        "total_available_inventory"
    )
    .fillna(0)
    .withColumn("processed_ts", current_timestamp())
)

# -----------------------------
# Write Gold table
# -----------------------------
(
    kpis.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_BUSINESS_KPIS)
)