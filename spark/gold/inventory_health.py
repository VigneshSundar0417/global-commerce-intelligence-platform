from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col

spark = (
    SparkSession.builder
    .appName("gold_inventory_health")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

inventory = spark.readStream.format("delta").load("data/silver/inventory")

latest_qty = (
    inventory
    .groupBy("item_id")
    .agg(max("new_quantity").alias("current_quantity"))
)

query = (
    latest_qty.writeStream
    .format("delta")
    .option("checkpointLocation", "data/gold/inventory_health/_checkpoint")
    .outputMode("complete")
    .start("data/gold/inventory_health")
)

query.awaitTermination()