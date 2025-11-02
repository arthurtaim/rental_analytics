"""
Инкрементальная выгрузка автомобилей (catalog_id = 8525) → stg.stg_cars
Airflow ≥ 3.0.3  •  Schedule: @daily
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
TABLE  = f"{SCHEMA}.stg_cars"


# ── helpers ────────────────────────────────────────────────────────────────
def _ensure_schema(cur):             # создаём схему stg при первом запуске
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")


def _fetch_elements(cid: int, token: str, sub: str) -> List[Dict]:
    url = f"https://{sub}.amocrm.ru/api/v4/catalogs/{cid}/elements"
    headers = {"Authorization": f"Bearer {token}"}
    out, page = [], 1
    while True:
        r = requests.get(url, headers=headers,
                         params={"page": page, "limit": 250, "with": "custom_fields"},
                         timeout=30)
        if r.status_code == 204 or not r.content.strip(): break
        if r.status_code == 429: time.sleep(1); continue
        r.raise_for_status()
        out.extend(r.json().get("_embedded", {}).get("elements", []))
        page += 1; time.sleep(0.15)
    return out


def _parse_car(raw: dict) -> dict:
    car = {
        "id": raw["id"], "model": None, "color": None, "year": None,
        "plate": None, "vin": None, "dailyrate": None,
    }
    for cf in raw.get("custom_fields_values", []):
        fid = cf["field_id"]; val = cf["values"][0]["value"]
        match fid:
            case 883383: car["model"]     = val
            case 883385: car["color"]     = val
            case 883387: car["year"]      = int(val)
            case 883389: car["plate"]     = val
            case 883391: car["vin"]       = val
            case 883393: car["dailyrate"] = float(val)
    return car


# ── tasks ──────────────────────────────────────────────────────────────────
def extract_cars(**ctx):
    load_dotenv("/opt/airflow/.env")
    sub, tok = os.getenv("AMO_SUBDOMAIN"), os.getenv("AMO_TOKEN")
    cars = [_parse_car(r) for r in _fetch_elements(8525, tok, sub)]
    with open("/tmp/cars.json", "w") as f: json.dump(cars, f, ensure_ascii=False)
    ctx["ti"].xcom_push(key="cnt", value=len(cars))
    print(f"[extract] {len(cars)} cars")


def load_cars(**ctx):
    load_dotenv("/opt/airflow/.env")
    with open("/tmp/cars.json") as f: cars = json.load(f)

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT", 5432),
        user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DATABASE"))
    cur = conn.cursor()
    _ensure_schema(cur)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id        BIGINT PRIMARY KEY,
            model     TEXT,
            color     TEXT,
            year      INT,
            plate     TEXT,
            vin       TEXT,
            dailyrate NUMERIC,
            load_date TIMESTAMP NOT NULL
        );
    """)

    new = 0
    for c in cars:
        cur.execute(f"""
            INSERT INTO {TABLE}
                   (id, model, color, year, plate, vin, dailyrate, load_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT(id) DO NOTHING;
        """, (c["id"], c["model"], c["color"], c["year"],
              c["plate"], c["vin"], c["dailyrate"]))
        new += cur.rowcount

    conn.commit(); cur.close(); conn.close()
    total = ctx["ti"].xcom_pull(key="cnt", task_ids="extract_cars")
    print(f"[load] {new}/{total} inserted")


# ── DAG ────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="daily_amocrm_load_cars",
    schedule="@daily",
    start_date=datetime(2025, 7, 31),
    catchup=False,
    tags=["amoCRM", "STG", "cars"],
    description="Инкрементальная загрузка автомобилей из amoCRM",
) as dag:
    t1 = PythonOperator(task_id="extract_cars",
                        python_callable=extract_cars,
                        retries=3, retry_delay=timedelta(minutes=1))
    t2 = PythonOperator(task_id="load_cars_to_pg",
                        python_callable=load_cars)
    t1 >> t2
