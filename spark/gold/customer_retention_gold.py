from pyspark.sql.functions import (
    col, to_date, countDistinct, min as _min, max as _max,
    when, current_timestamp
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = get_spark("customer_retention_gold")

SILVER_ORDERS = silver_path() + "orders"
GOLD_RETENTION = gold_path() + "customer_retention_daily"

# -----------------------------
# Load Silver orders
# -----------------------------
orders_df = spark.read.format("delta").load(SILVER_ORDERS)

# -----------------------------
# Prepare order-level fields
# -----------------------------
orders_clean = (
    orders_df
    .withColumn("order_date", to_date(col("order_timestamp")))
)

# -----------------------------
# First and last order per customer
# -----------------------------
customer_lifecycle = (
    orders_clean
    .groupBy("customer_id")
    .agg(
        _min("order_date").alias("first_order_date"),
        _max("order_date").alias("last_order_date")
    )
)

# -----------------------------
# Daily customer activity
# -----------------------------
daily_customers = (
    orders_clean
    .groupBy("order_date", "region")
    .agg(
        countDistinct("customer_id").alias("active_customers")
    )
)

# -----------------------------
# Join lifecycle + daily activity
# -----------------------------
retention = (
    daily_customers
    .join(
        customer_lifecycle,
        daily_customers.order_date == customer_lifecycle.last_order_date,
        "left"
    )
    .withColumn(
        "new_customers",
        when(col("order_date") == col("first_order_date"), 1).otherwise(0)
    )
    .withColumn(
        "returning_customers",
        when(
            (col("order_date") != col("first_order_date")) &
            (col("order_date") == col("last_order_date")),
            1
        ).otherwise(0)
    )
    .withColumn(
        "churn_risk_customers",
        when(
            (col("order_date") > col("last_order_date")),
            1
        ).otherwise(0)
    )
    .withColumn("processed_ts", current_timestamp())
)

# -----------------------------
# Write Gold table
# -----------------------------
(
    retention.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_RETENTION)
)