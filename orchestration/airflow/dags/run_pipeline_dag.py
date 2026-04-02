from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="run_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    bronze = TriggerDagRunOperator(
        task_id="trigger_bronze",
        trigger_dag_id="bronze_streaming",
    )

    silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_etl",
    )

    gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="gold_etl",
    )

    bronze >> silver >> gold