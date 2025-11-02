"""
daily_amocrm_load_leads.py
Инкрементальная выгрузка лидов (pipeline_id = 9877710) → stg.stg_leads
Работает с Airflow ≥ 3.0.3
"""

from __future__ import annotations
import json, os, time
from datetime import datetime, timedelta
from typing import List, Dict

import requests, psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

SCHEMA = "stg"
TABLE  = f"{SCHEMA}.stg_leads"


# ─────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────
def _ensure_schema(cur):                   # создаём схему stg
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")


def _fetch_leads(tok: str, sub: str) -> List[Dict]:
    """Постранично тянем лиды нужного pipeline."""
    url = f"https://{sub}.amocrm.ru/api/v4/leads"
    headers = {"Authorization": f"Bearer {tok}"}
    out, page = [], 1
    while True:
        r = requests.get(
            url,
            headers=headers,
            params={
                "page": page,
                "limit": 250,
                "with": "custom_fields",
                "filter[pipeline_id]": 9877710,
            },
            timeout=30,
        )
        if r.status_code == 204 or not r.content.strip():
            break
        if r.status_code == 429:
            time.sleep(1)
            continue
        r.raise_for_status()
        out.extend(r.json().get("_embedded", {}).get("leads", []))
        page += 1
        time.sleep(0.15)
    return out


def _parse_lead(raw: dict) -> dict:
    lead = {
        "id": raw["id"],
        "name": raw.get("name"),
        "status_id": raw["status_id"],
        "created_at": datetime.utcfromtimestamp(raw["created_at"]),
        "updated_at": datetime.utcfromtimestamp(raw["updated_at"]),
        # кастом-поля
        "model": None,
        "plate": None,
        "dailyrate": None,
        "client_name": None,
        "license": None,
        "rental_start": None,
        "rental_end": None,
        "budget": raw.get("price"),
    }
    for cf in raw.get("custom_fields_values", []):
        fid = cf["field_id"]
        val = cf["values"][0]["value"]
        match fid:
            case 884377: lead["model"] = val
            case 884379: lead["plate"] = val
            case 884381: lead["dailyrate"] = val
            case 884383: lead["client_name"] = val
            case 884385: lead["license"] = val
            case 883843: lead["rental_start"] = datetime.utcfromtimestamp(val)
            case 883845: lead["rental_end"] = datetime.utcfromtimestamp(val)
    return lead


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


# ─────────────────────────────────────────────────────────────
# tasks
# ─────────────────────────────────────────────────────────────
def extract_leads(**ctx):
    load_dotenv("/opt/airflow/.env")
    sub, tok = os.getenv("AMO_SUBDOMAIN"), os.getenv("AMO_TOKEN")

    leads = [_parse_lead(r) for r in _fetch_leads(tok, sub)]
    with open("/tmp/leads.json", "w") as f:
        json.dump([{k: _iso(v) for k, v in d.items()} for d in leads],
                  f, ensure_ascii=False)

    ctx["ti"].xcom_push(key="cnt", value=len(leads))
    print(f"[extract] {len(leads)} leads")


def load_leads(**ctx):
    load_dotenv("/opt/airflow/.env")
    with open("/tmp/leads.json") as f:
        leads = json.load(f)

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", 5432),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DATABASE"),
    )
    cur = conn.cursor()
    _ensure_schema(cur)

    # создаём таблицу при первом запуске
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id           BIGINT PRIMARY KEY,
            name         TEXT,
            status_id    BIGINT,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP,
            model        TEXT,
            plate        TEXT,
            dailyrate    TEXT,
            client_name  TEXT,
            license      TEXT,
            rental_start TIMESTAMP,
            rental_end   TIMESTAMP,
            budget       NUMERIC,
            load_date    TIMESTAMP NOT NULL
        );
        """
    )

    new = 0
    for l in leads:
        cur.execute(
            f"""
            INSERT INTO {TABLE} (
                id, name, status_id, created_at, updated_at,
                model, plate, dailyrate, client_name, license,
                rental_start, rental_end, budget, load_date)
            VALUES (
                %(id)s, %(name)s, %(status_id)s, %(created_at)s, %(updated_at)s,
                %(model)s, %(plate)s, %(dailyrate)s, %(client_name)s, %(license)s,
                %(rental_start)s, %(rental_end)s, %(budget)s, NOW())
            ON CONFLICT(id) DO NOTHING;
            """,
            l,
        )
        new += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    total = ctx["ti"].xcom_pull(key="cnt", task_ids="extract_leads")
    print(f"[load] {new}/{total} leads inserted")


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="daily_amocrm_load_leads",
    schedule="@daily",
    start_date=datetime(2025, 7, 31),
    catchup=False,
    tags=["amoCRM", "STG", "leads"],
    description="Инкрементальная загрузка лидов из amoCRM",
) as dag:
    t1 = PythonOperator(
        task_id="extract_leads",
        python_callable=extract_leads,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    t2 = PythonOperator(
        task_id="load_leads",
        python_callable=load_leads,
    )

    t1 >> t2
