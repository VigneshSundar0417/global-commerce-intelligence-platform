from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path, silver_path

spark = get_spark("inventory_silver")

# Paths
BRONZE_PATH = bronze_path() + "inventory"
SILVER_PATH = silver_path() + "inventory"

# Read Bronze as batch
bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
)

# Transformations
silver_df = (
    bronze_df
    .withColumn("last_updated_ts", to_timestamp(col("last_updated")))
    .withColumn("region", upper(col("region")))
    .withColumn("processed_ts", current_timestamp())
    .dropDuplicates(["inventory_id"])
)

# Write to Silver (overwrite is safe for batch)
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)