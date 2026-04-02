from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, min as _min, max as _max,
    datediff, lit
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = get_spark("customer_gold_ltv")

SILVER_ORDERS = silver_path() + "orders"
SILVER_PAYMENTS = silver_path() + "payments"
SILVER_EVENTS = silver_path() + "customer_events"
GOLD_CUSTOMER_LTV = gold_path() + "customer_ltv"

# -----------------------------
# Load Silver tables
# -----------------------------
orders_df = spark.read.format("delta").load(SILVER_ORDERS)
payments_df = spark.read.format("delta").load(SILVER_PAYMENTS)
events_df = spark.read.format("delta").load(SILVER_EVENTS)

# -----------------------------
# Revenue per customer
# -----------------------------
orders_enriched = (
    orders_df
    .withColumn("order_revenue", col("price") * col("quantity"))
)

revenue_agg = (
    orders_enriched
    .groupBy("customer_id")
    .agg(
        _sum("order_revenue").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        _min("order_ts").alias("first_order_ts"),
        _max("order_ts").alias("last_order_ts")
    )
)

# -----------------------------
# Successful payments per customer
# -----------------------------
payments_success = (
    payments_df
    .filter(col("payment_status") == "SUCCESS")
    .groupBy("customer_id")
    .agg(
        _sum("amount").alias("total_paid"),
        countDistinct("payment_id").alias("successful_payments")
    )
)

# -----------------------------
# Engagement score (simple)
# -----------------------------
engagement_agg = (
    events_df
    .groupBy("customer_id")
    .agg(
        countDistinct("event_id").alias("engagement_events")
    )
)

# -----------------------------
# Combine all metrics
# -----------------------------
ltv_df = (
    revenue_agg
    .join(payments_success, "customer_id", "left")
    .join(engagement_agg, "customer_id", "left")
    .withColumn("active_days", datediff(col("last_order_ts"), col("first_order_ts")))
    .withColumn("engagement_score", col("engagement_events"))
    .withColumn("ltv", col("total_revenue"))  # simple LTV = revenue
    .fillna(0)
)

# -----------------------------
# Write Gold table
# -----------------------------
(
    ltv_df.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_CUSTOMER_LTV)
)