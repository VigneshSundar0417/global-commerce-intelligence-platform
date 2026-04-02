from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, to_date
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = get_spark("orders_gold_daily")

SILVER_ORDERS = silver_path() + "orders"
GOLD_ORDERS_DAILY = gold_path() + "orders_daily_metrics"
print("SILVER_ORDERS =", SILVER_ORDERS)
# Read Silver orders (batch)
orders_silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_ORDERS)
)

# Derive order_date
orders_enriched = (
    orders_silver_df
    .withColumn("order_date", to_date(col("order_timestamp")))
)

# Aggregate to daily metrics
daily_metrics_df = (
    orders_enriched
    .groupBy("order_date", "region", "device_type", "currency")
    .agg(
        _sum(col("price") * col("quantity")).alias("total_revenue"),
        _sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders")
    )
)

# Write Gold table
(
    daily_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_ORDERS_DAILY)
)