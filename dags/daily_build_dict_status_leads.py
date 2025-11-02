from __future__ import annotations
import os, time
from datetime import datetime
import psycopg2
import psycopg2.extras as pgx
import requests
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator

SQL_DIR = "/opt/airflow/sql/dict"

def extract_statuses_to_stage() -> None:
    """
    Тянет статусы из amoCRM и пишет батчом в dict.dict_status_leads_stg.
    """
    load_dotenv("/opt/airflow/.env")
    subdomain = os.getenv("AMO_SUBDOMAIN")
    token = os.getenv("AMO_ACCESS_TOKEN")
    if not subdomain or not token:
        raise RuntimeError("AMO_SUBDOMAIN / AMO_ACCESS_TOKEN не заданы в /opt/airflow/.env")

    url = f"https://{subdomain}.amocrm.ru/api/v4/leads/pipelines"
    params = {"with": "statuses"}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    print(f"[amoCRM] GET {url} with=statuses")
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"amoCRM HTTP {r.status_code}: {r.text[:500]}")

    data = r.json()
    rows = []
    for pipe in data.get("_embedded", {}).get("pipelines", []):
        pid = pipe.get("id")
        pname = pipe.get("name")
        for st in pipe.get("_embedded", {}).get("statuses", []):
            rows.append((
                st.get("id"),
                pid,
                pname,
                st.get("name"),
                st.get("sort"),
                bool(st.get("is_archive", False)),
            ))
    print(f"[amoCRM] parsed rows: {len(rows)}")

    # write to staging
    load_dotenv("/opt/airflow/.env")
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DATABASE"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS dict;")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dict.dict_status_leads_stg (
                      status_id BIGINT,
                      pipeline_id BIGINT,
                      pipeline_name TEXT,
                      status_name TEXT,
                      status_sort INT,
                      is_archive BOOLEAN
                    );
                """)
                cur.execute("TRUNCATE TABLE dict.dict_status_leads_stg;")
            # batch insert
            with conn.cursor() as cur:
                pgx.execute_values(cur,
                    """
                    INSERT INTO dict.dict_status_leads_stg
                    (status_id, pipeline_id, pipeline_name, status_name, status_sort, is_archive)
                    VALUES %s
                    """,
                    rows, page_size=1000
                )
        print(f"[staging] inserted {len(rows)} rows into dict.dict_status_leads_stg")
    finally:
        conn.close()

def run_sql(path: str) -> None:
    """Выполнить SQL-файл (абсолютный путь)."""
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
    dag_id="daily_build_dict_status_leads",
    start_date=datetime(2025, 7, 31, 4, 55),
    schedule="@daily",
    catchup=False,
    tags=["DICT", "amoCRM", "statuses"],
    description="Загрузка словаря статусов лидов из amoCRM (pipelines?with=statuses) → dict",
) as dag:

    extract_to_stage = PythonOperator(
        task_id="extract_statuses_to_stage",
        python_callable=extract_statuses_to_stage,
    )

    merge_to_dict = PythonOperator(
        task_id="merge_dict_status_leads",
        python_callable=run_sql,
        op_args=[f"{SQL_DIR}/merge_dict_status_leads.sql"],
    )

    extract_to_stage >> merge_to_dict
