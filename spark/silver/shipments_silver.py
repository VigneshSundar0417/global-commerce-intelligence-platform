from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path, silver_path

spark = get_spark("shipments_silver")

# Paths
BRONZE_PATH = bronze_path() + "shipments"
SILVER_PATH = silver_path() + "shipments"
CHECKPOINT_PATH = SILVER_PATH + "/_checkpoint"

# Read Bronze as streaming Delta
bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

# Transformations
silver_df = (
    bronze_df
    .withColumn("shipment_ts", to_timestamp(col("shipment_timestamp")))
    .withColumn("shipment_status", upper(col("shipment_status")))
    .withColumn("processed_ts", current_timestamp())
    .withWatermark("shipment_ts", "30 minutes")
    .dropDuplicates(["shipment_id"])
)

# Write to Silver
query = (
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start(SILVER_PATH)
)

query.awaitTermination()