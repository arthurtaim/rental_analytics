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
    dag_id="daily_build_dds_dim",
    start_date=datetime(2025, 7, 31, 5, 0),
    schedule="@daily",
    catchup=False,
    tags=["DDS", "dim"],
    description="DDS dimensions (car, client, status) from ODS/DICT",
) as dag:

    wait_ods_cars = ExternalTaskSensor(
        task_id="wait_ods_merge_cars",
        external_dag_id="daily_load_ods_cars",
        external_task_id="merge_ods_cars",
        allowed_states=["success"], failed_states=["failed", "upstream_failed"],
        execution_delta=timedelta(0), poke_interval=60, timeout=60*60, mode="reschedule",
    )
    wait_ods_clients = ExternalTaskSensor(
        task_id="wait_ods_merge_clients",
        external_dag_id="daily_load_ods_clients",
        external_task_id="merge_ods_clients",
        allowed_states=["success"], failed_states=["failed", "upstream_failed"],
        execution_delta=timedelta(0), poke_interval=60, timeout=60*60, mode="reschedule",
    )
    wait_dict_status = ExternalTaskSensor(
        task_id="wait_dict_status_leads",
        external_dag_id="daily_build_dict_status_leads",
        external_task_id="merge_dict_status_leads",
        allowed_states=["success"], failed_states=["failed", "upstream_failed"],
        execution_delta=timedelta(0), poke_interval=60, timeout=60*60, mode="reschedule",
    )

    merge_dim_car = PythonOperator(
        task_id="merge_dim_car",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_dim_car.sql"],
    )
    merge_dim_client = PythonOperator(
        task_id="merge_dim_client",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_dim_client.sql"],
    )
    merge_dim_status = PythonOperator(
        task_id="merge_dim_status",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_dim_status.sql"],
    )

    wait_ods_cars    >> merge_dim_car
    wait_ods_clients >> merge_dim_client
    wait_dict_status >> merge_dim_status
