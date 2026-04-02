from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import os

BRONZE = "/opt/airflow/data/bronze"
SILVER = "/opt/airflow/data/silver"
GOLD = "/opt/airflow/data/gold"

def check_new_data():
    latest = max(os.path.getmtime(os.path.join(SILVER, f)) for f in os.listdir(SILVER))
    return latest

def build_gold_tables():
    con = duckdb.connect("/opt/airflow/warehouse/gold.duckdb")
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_sales AS
        SELECT
            order_date,
            SUM(total_amount) AS total_sales,
            COUNT(*) AS order_count
        FROM read_parquet('/opt/airflow/data/silver/orders/*.parquet')
        GROUP BY order_date
        ORDER BY order_date;
    """)
    con.close()

with DAG(
    dag_id="gold_batch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",  # hourly
    catchup=False,
) as dag:

    wait_for_streaming = PythonOperator(
        task_id="wait_for_streaming",
        python_callable=check_new_data,
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold_tables,
    )

    wait_for_streaming >> build_gold