from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "vicky",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_etl_spark",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    silver_etl = SparkSubmitOperator(
        task_id="run_silver_etl",
        application="/opt/airflow/project/spark/silver/silver_all_tables.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
        },
    )