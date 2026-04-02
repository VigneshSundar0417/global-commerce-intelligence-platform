#!/bin/bash

PKGS="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0"

spark-submit \
  --packages $PKGS \
  /home/vicky/global-commerce-intelligence-platform/spark/bronze/bronze_all_streams.py