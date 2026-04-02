from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import boto3
import duckdb
import requests

# ---------- CONFIG ----------
S3_BUCKET = "global-commerce-data"
SILVER_ORDERS_PREFIX = "silver/orders/"
GOLD_DUCKDB_PATH = "/opt/airflow/warehouse/gold.duckdb"

AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
DASHBOARD_REFRESH_URL = os.getenv("DASHBOARD_REFRESH_URL", "http://dashboard:8501/refresh")

# ---------- HELPERS ----------

def wait_for_new_silver(**context):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=SILVER_ORDERS_PREFIX,
    )
    contents = resp.get("Contents", [])
    if not contents:
        raise ValueError("No Silver data found in S3")

    latest = max(obj["LastModified"] for obj in contents)
    context["ti"].xcom_push(key="latest_silver_ts", value=latest.timestamp())


def build_gold_tables():
    con = duckdb.connect(GOLD_DUCKDB_PATH)
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(f"SET s3_region='{AWS_REGION}';")
    con.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID', '')}';")
    con.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY', '')}';")

    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_sales AS
        SELECT
            order_date,
            SUM(total_amount) AS total_sales,
            COUNT(*) AS order_count
        FROM read_parquet('s3://global-commerce-data/silver/orders/*.parquet')
        GROUP BY order_date
        ORDER BY order_date;
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS top_customers AS
        SELECT
            customer_id,
            SUM(total_amount) AS total_spent,
            COUNT(*) AS order_count
        FROM read_parquet('s3://global-commerce-data/silver/orders/*.parquet')
        GROUP BY customer_id
        ORDER BY total_spent DESC;
    """)

    con.close()


def data_quality_checks():
    con = duckdb.connect(GOLD_DUCKDB_PATH)

    # Example: no negative sales
    res = con.execute("""
        SELECT COUNT(*) 
        FROM daily_sales 
        WHERE total_sales < 0;
    """).fetchone()[0]
    if res > 0:
        raise ValueError(f"Data quality failed: {res} rows with negative total_sales")

    # Example: no null order_date
    res = con.execute("""
        SELECT COUNT(*) 
        FROM daily_sales 
        WHERE order_date IS NULL;
    """).fetchone()[0]
    if res > 0:
        raise ValueError(f"Data quality failed: {res} rows with NULL order_date")

    con.close()


def train_model():
    # Placeholder: import your own ML training module
    # from ml.training import train
    # train()
    print("Training model on Gold tables (hook into your ML code here).")


def score_model():
    # Placeholder: import your own scoring module
    # from ml.scoring import score
    # score()
    print("Scoring with latest model (hook into your ML code here).")


def refresh_dashboard():
    try:
        resp = requests.post(DASHBOARD_REFRESH_URL, timeout=10)
        if resp.status_code != 200:
            raise ValueError(f"Dashboard refresh failed: {resp.status_code} {resp.text}")
    except Exception as e:
        raise RuntimeError(f"Dashboard refresh error: {e}")


# ---------- DAG DEFINITION ----------

default_args = {
    "owner": "vicky",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="global_commerce_production_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",  # hourly; change to daily if you want
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_streaming = PythonOperator(
        task_id="wait_for_new_silver_data",
        python_callable=wait_for_new_silver,
        provide_context=True,
    )

    build_gold = PythonOperator(
        task_id="build_gold_tables",
        python_callable=build_gold_tables,
    )

    dq_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
    )

    train_ml = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    score_ml = PythonOperator(
        task_id="score_model",
        python_callable=score_model,
    )

    refresh_dash = PythonOperator(
        task_id="refresh_dashboard",
        python_callable=refresh_dashboard,
    )

    end = EmptyOperator(task_id="end")

    # Orchestration graph
    start >> wait_for_streaming >> build_gold >> dq_checks
    dq_checks >> [train_ml, score_ml] >> refresh_dash >> end