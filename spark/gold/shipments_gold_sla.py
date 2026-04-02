import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    when,
    current_timestamp,
    expr
)
from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = (
    SparkSession.builder
    .appName("shipments_gold_sla")
    .getOrCreate()
)

# Read Silver shipments table
shipments = (
    spark.read.format("delta")
    .load(silver_path() + "shipments")
)

# Convert shipment timestamp
shipments = shipments.withColumn(
    "ship_ts",
    to_timestamp(col("shipment_timestamp"))
)

# Extract ship date
shipments = shipments.withColumn(
    "ship_date",
    to_date(col("shipment_timestamp"))
)

# SLA logic:
# If delivered, compute hours since shipment_timestamp
# If not delivered, delay_hours = null
shipments = shipments.withColumn(
    "delay_hours",
    when(
        col("shipment_status") == "DELIVERED",
        (current_timestamp().cast("bigint") - col("shipment_timestamp").cast("bigint")) / 3600
    ).otherwise(None)
)

# On-time logic:
# If delivered AND delay_hours <= 48 hours → on time
shipments = shipments.withColumn(
    "is_on_time",
    when(
        (col("shipment_status") == "DELIVERED") &
        (col("delay_hours") <= 48),
        True
    ).otherwise(False)
)

# Write Gold table
(
    shipments.write.format("delta")
    .mode("overwrite")
    .save(gold_path() + "shipments_sla")
)

spark.stop()