from __future__ import annotations
import os
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

SQL_DIR = "/opt/airflow/sql/dds"

def run_sql(path: str) -> None:
    print(f"[run_sql] {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    load_dotenv("/opt/airflow/.env")
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DATABASE"),
    )
    try:
        with conn, conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
            cur.execute(f.read())
    finally:
        conn.close()

with DAG(
    dag_id="daily_build_dds_fact",
    start_date=datetime(2025, 7, 31, 5, 30),
    schedule="@daily",
    catchup=False,
    tags=["DDS", "fact"],
    description="DDS facts: rentals, lead_status_change, fleet_usage_day",
) as dag:

    wait_dim_car = ExternalTaskSensor(
        task_id="wait_dim_car",
        external_dag_id="daily_build_dds_dim",
        external_task_id="merge_dim_car",
        allowed_states=["success"], failed_states=["failed", "upstream_failed"],
        execution_delta=timedelta(0), poke_interval=60, timeout=60*60, mode="reschedule",
    )
    wait_dim_client = ExternalTaskSensor(
        task_id="wait_dim_client",
        external_dag_id="daily_build_dds_dim",
        external_task_id="merge_dim_client",
        allowed_states=["success"], failed_states=["failed", "upstream_failed"],
        execution_delta=timedelta(0), poke_interval=60, timeout=60*60, mode="reschedule",
    )
    wait_dim_status = ExternalTaskSensor(
        task_id="wait_dim_status",
        external_dag_id="daily_build_dds_dim",
        external_task_id="merge_dim_status",
        allowed_states=["success"], failed_states=["failed", "upstream_failed"],
        execution_delta=timedelta(0), poke_interval=60, timeout=60*60, mode="reschedule",
    )

    merge_fact_rentals = PythonOperator(
        task_id="merge_fact_rentals",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_fact_rentals.sql"],
    )
    merge_fact_lead_status_change = PythonOperator(
        task_id="merge_fact_lead_status_change",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_fact_lead_status_change.sql"],
    )
    merge_fact_fleet_usage_day = PythonOperator(
        task_id="merge_fact_fleet_usage_day",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_fact_fleet_usage_day.sql"],
    )

    [wait_dim_car, wait_dim_client, wait_dim_status] >> merge_fact_rentals
    [wait_dim_car, wait_dim_client]                  >> merge_fact_fleet_usage_day
    [wait_dim_status]                                >> merge_fact_lead_status_change
