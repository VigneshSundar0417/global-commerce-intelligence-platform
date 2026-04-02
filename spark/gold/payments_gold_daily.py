from pyspark.sql.functions import (
    col, sum as _sum, count, countDistinct, to_date, upper
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = get_spark("payments_gold_daily")

SILVER_PAYMENTS = silver_path() + "payments"
GOLD_PAYMENTS_DAILY = gold_path() + "payments_daily_metrics"

# Read Silver payments (batch)
payments_df = (
    spark.read
    .format("delta")
    .load(SILVER_PAYMENTS)
)

# Normalize + derive date
payments_clean = (
    payments_df
    .withColumn("payment_date", to_date(col("payment_timestamp")))
    .withColumn("payment_status", upper(col("payment_status")))
    .withColumn("payment_method", upper(col("payment_method")))
)

# Daily payment metrics
daily_metrics = (
    payments_clean
    .groupBy("payment_date", "region", "currency", "payment_method")
    .agg(
        countDistinct("payment_id").alias("total_payments"),
        count(col("payment_id")).alias("payment_events"),
        _sum(
            col("amount").cast("double")
        ).alias("total_amount"),
        count(
            col("payment_status") == "SUCCESS"
        ).alias("success_count"),
        count(
            col("payment_status") == "FAILED"
        ).alias("failed_count")
    )
)

# Write Gold table
(
    daily_metrics.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PAYMENTS_DAILY)
)