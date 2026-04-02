#!/bin/bash

echo "🚀 Starting Global Commerce Intelligence Platform..."

mkdir -p logs

# --- Start Kafka ---
echo "📦 Starting Kafka..."
cd kafka
docker compose up -d
cd ..

sleep 5

# --- Start Bronze Streaming ---
echo "🥉 Starting Bronze streaming..."
cd spark/bronze
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
  bronze_all_streams.py > ../../logs/bronze.log 2>&1 &
cd ../..

sleep 3

# --- Start Silver ---
echo "🥈 Running Silver ETL..."
cd spark/silver
spark-submit silver_all_tables.py > ../../logs/silver.log 2>&1 &
cd ../..

sleep 3

# --- Start Gold ---
echo "🥇 Running Gold ETL..."
cd spark/gold
spark-submit gold_all_tables.py > ../../logs/gold.log 2>&1 &
cd ../..

sleep 3

# --- Start Simulators ---
echo "🎛️ Starting event simulators..."
cd event_simulator
./all_simulators.sh
cd ..

echo ""
echo "✅ Platform started successfully!"
echo "📁 Logs available in ./logs/"