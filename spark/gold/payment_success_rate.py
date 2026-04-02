from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = (
    SparkSession.builder
    .appName("gold_payment_success_rate")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

payments = spark.readStream.format("delta").load("data/silver/payments")

metrics = (
    payments
    .groupBy("payment_method")
    .agg(
        count("*").alias("total_attempts"),
        count(col("payment_status") == "success").alias("success_count")
    )
)

query = (
    metrics.writeStream
    .format("delta")
    .option("checkpointLocation", "data/gold/payment_success_rate/_checkpoint")
    .outputMode("complete")
    .start("data/gold/payment_success_rate")
)

query.awaitTermination()