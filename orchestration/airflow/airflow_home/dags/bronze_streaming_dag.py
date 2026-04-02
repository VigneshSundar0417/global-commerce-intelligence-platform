from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_SUBMIT = (
    "spark-submit "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0 "
    "/home/vicky/global-commerce-intelligence-platform/spark/bronze/bronze_all_streams.py"
)

with DAG(
    dag_id="bronze_streaming",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    bronze = BashOperator(
        task_id="bronze_streaming_task",
        bash_command=SPARK_SUBMIT,
    )