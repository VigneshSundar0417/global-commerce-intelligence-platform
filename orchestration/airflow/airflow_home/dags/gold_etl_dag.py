from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_SUBMIT = (
    "spark-submit "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0 "
    "/home/vicky/global-commerce-intelligence-platform/spark/gold/gold_all_tables.py"
)

with DAG(
    dag_id="gold_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_gold = BashOperator(
        task_id="run_gold_etl",
        bash_command=SPARK_SUBMIT,
    )