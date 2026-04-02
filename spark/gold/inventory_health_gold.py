from pyspark.sql.functions import (
    col, sum as _sum, to_date, current_timestamp,
    when
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import silver_path, gold_path

spark = get_spark("inventory_health_gold")

SILVER_INVENTORY = silver_path() + "inventory"
GOLD_INVENTORY_HEALTH = gold_path() + "inventory_health"

# Read Silver inventory (batch)
inv_df = (
    spark.read
    .format("delta")
    .load(SILVER_INVENTORY)
)

# Derive date
inv_clean = (
    inv_df
    .withColumn("inventory_date", to_date(col("last_updated")))
)

# Inventory health metrics
inventory_health = (
    inv_clean
    .groupBy("inventory_date", "item_id", "warehouse_id", "region")
    .agg(
        _sum("quantity_available").alias("available_qty"),
        _sum("quantity_reserved").alias("reserved_qty")
    )
    .withColumn("total_qty", col("available_qty") + col("reserved_qty"))
    .withColumn(
        "stockout_risk",
        when(col("available_qty") <= 5, "HIGH")
        .when(col("available_qty") <= 20, "MEDIUM")
        .otherwise("LOW")
    )
    .withColumn(
        "overstock_risk",
        when(col("available_qty") >= 500, "HIGH")
        .when(col("available_qty") >= 200, "MEDIUM")
        .otherwise("LOW")
    )
    .withColumn("processed_ts", current_timestamp())
)

# Write Gold table
(
    inventory_health.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_INVENTORY_HEALTH)
)