import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    to_timestamp,
    to_date,
    expr
)
from pyspark.sql.functions import (
    col, count, countDistinct, to_date, upper, current_timestamp
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = get_spark("customer_engagement_gold")

SILVER_EVENTS = silver_path() + "customer_events"
GOLD_ENGAGEMENT = gold_path() + "customer_engagement_daily"

# Read Silver customer events (batch)
events_df = (
    spark.read
    .format("delta")
    .load(SILVER_EVENTS)
)

# Normalize + derive date
events_clean = (
    events_df
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("event_type", upper(col("event_type")))
    .withColumn("device_type", upper(col("device_type")))
)

# Daily engagement metrics
engagement_daily = (
    events_clean
    .groupBy("event_date", "region", "device_type")
    .agg(
        countDistinct("customer_id").alias("active_customers"),
        count("event_id").alias("total_events"),
        count(when(col("event_type") == "VIEW", True)).alias("views"),
        count(when(col("event_type") == "CLICK", True)).alias("clicks"),
        count(when(col("event_type") == "ADD_TO_CART", True)).alias("add_to_cart"),
        count(when(col("event_type") == "PURCHASE", True)).alias("purchase_events")
    )
    .withColumn("engagement_score", col("total_events"))
    .withColumn("processed_ts", current_timestamp())
)

# Write Gold table
(
    engagement_daily.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_ENGAGEMENT)
)