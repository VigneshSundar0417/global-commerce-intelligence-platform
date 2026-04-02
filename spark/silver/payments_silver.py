from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper
)

from spark.utils.spark_session import get_spark
from spark.utils.paths import bronze_path, silver_path

spark = get_spark("payments_silver")

# Paths
BRONZE_PATH = bronze_path() + "payments"
SILVER_PATH = silver_path() + "payments"
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
    .withColumn("payment_ts", to_timestamp(col("payment_timestamp")))
    .withColumn("currency", upper(col("currency")))
    .withColumn("processed_ts", current_timestamp())
    .withWatermark("payment_ts", "10 minutes")
    .dropDuplicates(["payment_id"])
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