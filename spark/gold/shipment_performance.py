from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, datediff

spark = (
    SparkSession.builder
    .appName("gold_shipment_performance")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

shipments = spark.readStream.format("delta").load("data/silver/shipments")

perf = (
    shipments
    .withColumn("delivery_days", datediff(col("estimated_delivery_ts"), col("shipment_timestamp
")))
    .groupBy("carrier")
    .agg(avg("delivery_days").alias("avg_delivery_days"))
)

query = (
    perf.writeStream
    .format("delta")
    .option("checkpointLocation", "data/gold/shipment_performance/_checkpoint")
    .outputMode("complete")
    .start("data/gold/shipment_performance")
)

query.awaitTermination()