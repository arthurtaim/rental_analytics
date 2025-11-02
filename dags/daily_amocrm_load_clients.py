"""
Инкрементальная выгрузка клиентов (catalog_id = 8527) из amoCRM
→ stg.stg_clients (PostgreSQL).

• Airflow ≥ 3.0.3      • schedule: @daily
• Сохраняет только новые id, load_date = NOW()
"""

from __future__ import annotations
import json, os, time
from datetime import datetime, timedelta, date
from typing import List, Dict

import requests, psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

SCHEMA = "stg"
TABLE  = f"{SCHEMA}.stg_clients"


# ───────────────────── helpers ──────────────────────
def _ensure_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")


def _fetch(cid: int, tok: str, sub: str) -> List[Dict]:
    url = f"https://{sub}.amocrm.ru/api/v4/catalogs/{cid}/elements"
    headers = {"Authorization": f"Bearer {tok}"}
    out, page = [], 1
    while True:
        r = requests.get(url, headers=headers,
                         params={"page": page, "limit": 250, "with": "custom_fields"},
                         timeout=30)
        if r.status_code == 204 or not r.content.strip():
            break
        if r.status_code == 429:
            time.sleep(1)
            continue
        r.raise_for_status()
        out.extend(r.json().get("_embedded", {}).get("elements", []))
        page += 1
        time.sleep(0.15)          # 7 req/s интеграция
    return out


def _parse_client(raw: dict) -> dict:
    cli = {"id": raw["id"], "gender": None, "phone": None, "email": None,
           "nationality": None, "birth_date": None,
           "license": None, "client_name": None}
    for cf in raw.get("custom_fields_values", []):
        fid = cf["field_id"]; val = cf["values"][0]["value"]
        match fid:
            case 883521: cli["gender"]      = val
            case 883523: cli["phone"]       = val
            case 883525: cli["email"]       = val
            case 883527: cli["nationality"] = val
            case 883529: cli["birth_date"]  = datetime.utcfromtimestamp(
                                                int(val)).date()
            case 883531: cli["license"]     = val
            case 883533: cli["client_name"] = val
    return cli


def _iso(v):
    """Преобразует date/datetime в ISO-строку для json.dump"""
    return v.isoformat() if isinstance(v, (date, datetime)) else v


# ───────────────────── tasks ────────────────────────
def extract_clients(**ctx):
    load_dotenv("/opt/airflow/.env")
    sub, tok = os.getenv("AMO_SUBDOMAIN"), os.getenv("AMO_TOKEN")

    clients = [_parse_client(r) for r in _fetch(8527, tok, sub)]

    with open("/tmp/clients.json", "w") as f:
        json.dump([{k: _iso(v) for k, v in c.items()} for c in clients],
                  f, ensure_ascii=False)

    ctx["ti"].xcom_push(key="cnt", value=len(clients))
    print(f"[extract] {len(clients)} clients")


def load_clients(**ctx):
    load_dotenv("/opt/airflow/.env")
    with open("/tmp/clients.json") as f:
        clients = json.load(f)

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", 5432),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DATABASE"),
    )
    cur = conn.cursor()
    _ensure_schema(cur)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id           BIGINT PRIMARY KEY,
            gender       TEXT,
            phone        TEXT,
            email        TEXT,
            nationality  TEXT,
            birth_date   DATE,
            license      TEXT,
            client_name  TEXT,
            load_date    TIMESTAMP NOT NULL
        );
    """)

    new = 0
    for c in clients:
        cur.execute(f"""
            INSERT INTO {TABLE}
                   (id, gender, phone, email, nationality, birth_date,
                    license, client_name, load_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT(id) DO NOTHING;
        """, (c["id"], c["gender"], c["phone"], c["email"], c["nationality"],
              c["birth_date"], c["license"], c["client_name"]))
        new += cur.rowcount

    conn.commit(); cur.close(); conn.close()

    total = ctx["ti"].xcom_pull(key="cnt", task_ids="extract_clients")
    print(f"[load] {new}/{total} clients inserted")


# ───────────────────── DAG ──────────────────────────
with DAG(
    dag_id="daily_amocrm_load_clients",
    schedule="@daily",
    start_date=datetime(2025, 7, 31),
    catchup=False,
    tags=["amoCRM", "STG", "clients"],
    description="Инкрементальная загрузка клиентов из amoCRM",
) as dag:
    t_extract = PythonOperator(
        task_id="extract_clients",
        python_callable=extract_clients,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    t_load = PythonOperator(
        task_id="load_clients",
        python_callable=load_clients,
    )

    t_extract >> t_load
