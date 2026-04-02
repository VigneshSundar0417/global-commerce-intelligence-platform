#!/bin/bash

echo "======================================"
echo "   GLOBAL COMMERCE PIPELINE RUNNER"
echo "======================================"

# PACKAGES REQUIRED FOR ALL JOBS
PKGS="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0"

echo ""
echo ">>> Starting Kafka (Docker)..."
cd kafka
docker compose up -d
cd ..

sleep 5

echo ""
echo ">>> Creating Kafka topics..."
python3 kafka/create_topics.py

sleep 2

# ---------------- EVENT GENERATORS ----------------
start_gen() {
  name="$1"
  script="$2"
  echo ">>> Starting generator: $name"
  python3 event_simulator/$script &
}

echo ""
echo ">>> Starting Event Generators..."
start_gen "orders" "generate_orders.py"
start_gen "payments" "generate_payments.py"
start_gen "inventory" "generate_inventory.py"
start_gen "shipments" "generate_shipments.py"
start_gen "customer_events" "generate_customer_events.py"

sleep 3

# ---------------- SPARK JOBS ----------------
echo ""
echo ">>> Running BRONZE (Kafka Streams)..."
spark-submit --packages $PKGS spark/bronze/bronze_all_streams.py &

sleep 3

echo ""
echo ">>> Running SILVER (Batch Transformations)..."
spark-submit --packages $PKGS spark/silver/silver_all_tables.py &

sleep 3

echo ""
echo ">>> Running GOLD (Analytics Models)..."
spark-submit --packages $PKGS spark/gold/gold_all_tables.py &

echo ""
echo "======================================"
echo "   PIPELINE STARTED SUCCESSFULLY"
echo "======================================"