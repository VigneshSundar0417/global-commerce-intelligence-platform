from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (
    SparkSession.builder
    .appName("gold_customer_engagement")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

events = spark.readStream.format("delta").load("data/silver/customer_events")

engagement = (
    events
    .groupBy("event_type", "region")
    .agg(count("*").alias("event_count"))
)

query = (
    engagement.writeStream
    .format("delta")
    .option("checkpointLocation", "data/gold/customer_engagement/_checkpoint")
    .outputMode("complete")
    .start("data/gold/customer_engagement")
)

query.awaitTermination()