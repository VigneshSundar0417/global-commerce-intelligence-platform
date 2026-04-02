import os
from pyspark.sql import SparkSession

def get_spark(app_name="GlobalCommerce"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # ----------------------------------------------------
    # ENABLE S3 ONLY IF AWS CREDENTIALS ARE PRESENT
    # ----------------------------------------------------
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    if aws_key and aws_secret:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", aws_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    return builder.getOrCreate()