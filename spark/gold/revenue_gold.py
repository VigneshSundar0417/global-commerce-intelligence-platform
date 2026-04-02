from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = (
    SparkSession.builder
    .appName("gold_daily_revenue")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

orders = spark.readStream.format("delta").load("data/silver/orders")

daily_rev = (
    orders
    .groupBy("order_date", "region")
    .agg(sum(col("price") * col("quantity")).alias("total_revenue"))
)

query = (
    daily_rev.writeStream
    .format("delta")
    .option("checkpointLocation", "data/gold/daily_revenue/_checkpoint")
    .outputMode("complete")
    .start("data/gold/daily_revenue")
)

query.awaitTermination()