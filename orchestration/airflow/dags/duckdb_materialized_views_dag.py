from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_SUBMIT = (
    "spark-submit "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0 "
    "/home/vicky/global-commerce-intelligence-platform/spark/duckdb/refresh_views.py"
)

with DAG(
    dag_id="duckdb_materialized_views",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    refresh = BashOperator(
        task_id="refresh_duckdb_views_task",
        bash_command=SPARK_SUBMIT,
    )