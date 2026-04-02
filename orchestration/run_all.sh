#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/orchestration/logs"
PID_DIR="$ROOT_DIR/orchestration/pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

PYTHON="$ROOT_DIR/venv/bin/python"
SPARK_SUBMIT="$ROOT_DIR/venv/bin/spark-submit"

echo "=== Starting Global Commerce Platform ==="
echo "Root: $ROOT_DIR"
echo

# ---------------- KAFKA (DOCKER) ----------------
echo "[1/6] Starting Kafka (docker-compose)..."
cd "$ROOT_DIR/kafka"
docker-compose up -d >> "$LOG_DIR/kafka.log" 2>&1

sleep 5

echo "[2/6] Creating Kafka topics..."
$PYTHON "$ROOT_DIR/kafka/create_topics.py" >> "$LOG_DIR/kafka_topics.log" 2>&1 &

# ---------------- EVENT GENERATORS ----------------
cd "$ROOT_DIR"

start_gen() {
  name="$1"
  script="$2"
  echo "Starting generator: $name"
  $PYTHON "$ROOT_DIR/event_simulator/$script" >> "$LOG_DIR/${name}.log" 2>&1 &
  echo $! > "$PID_DIR/${name}.pid"
}

echo "[3/6] Starting event generators..."
start_gen "orders_generator" "generate_orders.py"
start_gen "payments_generator" "generate_payments.py"
start_gen "inventory_generator" "generate_inventory.py"
start_gen "shipments_generator" "generate_shipments.py"
start_gen "customer_events_generator" "generate_customer_events.py"

# ---------------- SPARK JOBS (BRONZE / SILVER / GOLD) ----------------
start_spark() {
  name="$1"
  script="$2"
  echo "Starting Spark job: $name"
  $SPARK_SUBMIT \
    --jars "$ROOT_DIR/spark/jars/delta-spark_2.12-3.2.0.jar,$ROOT_DIR/spark/jars/delta-storage-3.2.0.jar,$ROOT_DIR/spark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,$ROOT_DIR/spark/jars/kafka-clients-3.5.1.jar,$ROOT_DIR/spark/jars/commons-pool2-2.11.1.jar,$ROOT_DIR/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar" \
    "$ROOT_DIR/$script" >> "$LOG_DIR/${name}.log" 2>&1 &
  echo $! > "$PID_DIR/${name}.pid"
}

echo "[4/6] Starting Spark streaming pipelines..."
start_spark "bronze_all_streams" "spark/bronze/bronze_all_streams.py"
start_spark "silver_all_tables" "spark/silver/silver_all_tables.py"
start_spark "gold_all_streams" "spark/gold/gold_all_streams.py"

# ---------------- STREAMLIT DASHBOARD ----------------
echo "[5/6] Starting Streamlit dashboard..."
cd "$ROOT_DIR/dashboards/streamlit"
"$ROOT_DIR/venv/bin/streamlit" run app.py >> "$LOG_DIR/streamlit.log" 2>&1 &
echo $! > "$PID_DIR/streamlit.pid"

echo
echo "[6/6] All components started."
echo "Logs: $LOG_DIR"
echo "PIDs:  $PID_DIR"