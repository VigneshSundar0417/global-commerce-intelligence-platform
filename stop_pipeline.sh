#!/bin/bash

echo "🛑 Stopping Global Commerce Intelligence Platform..."

# Stop simulators
echo "🛑 Stopping simulators..."
killall python3 2>/dev/null

# Stop Spark jobs
echo "🛑 Stopping Spark jobs..."
pkill -f bronze_all_streams.py
pkill -f silver_all_tables.py
pkill -f gold_all_tables.py

# Stop Kafka
echo "🛑 Stopping Kafka..."
cd kafka
docker compose down
cd ..

echo "✅ All components stopped."