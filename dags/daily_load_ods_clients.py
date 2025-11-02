from __future__ import annotations
import os
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

SQL_DIR = "/opt/airflow/sql/ods"

def run_sql(path: str) -> None:
    load_dotenv("/opt/airflow/.env")
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DATABASE"),
    )
    try:
        with conn, conn.cursor() as cur:
            with open(path, "r", encoding="utf-8") as f:
                sql = f.read()
            print(f"[run_sql] executing: {path}")
            cur.execute(sql)
    finally:
        conn.close()

with DAG(
    dag_id="daily_load_ods_clients",
    start_date=datetime(2025, 7, 31, 4, 0),
    schedule="@daily",
    catchup=False,
    tags=["ODS", "clients"],
    description="STG -> ODS (clients) SCD2 merge",
) as dag:

    wait_stg = ExternalTaskSensor(
        task_id="wait_stg_clients_loaded",
        external_dag_id="daily_amocrm_load_clients",
        external_task_id="load_clients_to_pg",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        execution_delta=timedelta(0),
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )



    merge_ods = PythonOperator(
        task_id="merge_ods_clients",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_ods_clients.sql"],
    )

    wait_stg >> merge_ods
