# Rental Analytics Data Platform

## Overview
This repository contains the source code and helper utilities for the **Rental Analytics** data platform. It provides an end-to-end ELT pipeline that ingests operational data from amoCRM, persists it in a PostgreSQL warehouse, and prepares business-friendly data marts for analytics about the short-term car rental business.

The project is organised around three data layers:

* **STG (staging)** – Incremental extracts from amoCRM pipelines are landed in `stg.*` tables.
* **ODS (operational data store)** – Slowly changing historical tables (`ods.*`) are built on top of the staging layer.
* **DDS (data delivery layer)** – Dimensional models and facts (`dds.*`) consolidate the curated history for BI and reporting.

Apache Airflow orchestrates the daily workflows, while standalone loader scripts help maintain the amoCRM metadata used by the pipelines.

## Repository layout

| Path | Description |
|------|-------------|
| `dags/` | Airflow DAGs that implement daily ingestion to STG, ODS and DDS layers. Each DAG uses only built-in Python operators and `psycopg2` per the internal convention. |
| `sql/ods/` | SQL scripts that initialise and merge SCD-2 history tables in the ODS layer. |
| `sql/dds/` | SQL scripts for building dimensional tables and fact tables in the DDS layer. |
| `sql/dict/` | SQL helpers for synchronising dictionary data such as lead status lookups. |
| `Загрузчики/` | Utility scripts for managing amoCRM catalogues, custom fields, and demo data generation. These scripts rely on `.env` secrets and `python-dotenv`. |
| `Исходные/` | Static source files used by the loaders (for example, calendar demos or catalog element payloads). |
| `Логи/` | Placeholder directory for Airflow run logs. |
| `Даги инструкция` | Internal checklist that documents the house style for implementing Airflow DAGs in this project. |

## Airflow DAGs

All DAGs live under `dags/` and assume they are deployed to an Airflow environment (3.0.3+) where the repository is mounted to `/opt/airflow`. Highlights:

* `daily_amocrm_load_*.py` – Extract leads, clients and cars from amoCRM via REST API and land them in `stg.*` tables.
* `daily_load_ods_*.py` – Apply SCD-2 merge logic to populate `ods.*` history tables.
* `daily_build_dds_dim.py` & `daily_build_dds_fact.py` – Build dimensional and fact tables in the DDS schema once the upstream ODS jobs finish.
* `daily_build_dict_status_leads.py` – Refreshes the status dictionary for lead pipelines.

The DAGs follow conventions captured in `Даги инструкция`: they rely on `PythonOperator`, connect to PostgreSQL with credentials from `/opt/airflow/.env`, and coordinate inter-DAG dependencies via `ExternalTaskSensor`.

## SQL routines

The SQL scripts are idempotent and are executed from the DAGs through lightweight Python helpers. Each script is responsible for a concrete layer:

* **ODS merge scripts** handle opening/closing SCD-2 validity windows and stamping `load_date` metadata (for example, `sql/ods/merge_ods_leads.sql`).
* **DDS merge scripts** build dimension tables (`merge_dim_car.sql`, `merge_dim_client.sql`, `merge_dim_status.sql`) and facts (`merge_fact_lead_status_change.sql`, `merge_fact_rentals.sql`, etc.).
* **Dictionary scripts** keep shared lookup tables, such as amoCRM lead statuses, in sync with the source system.

## Loader utilities

Scripts in `Загрузчики/` are meant to be run manually when configuring or seeding amoCRM for demos:

* `cleaner.py` removes any extraneous custom lead fields, keeping only the required seven fields used by the pipeline.
* `helper_fields.py` ensures that all required custom lead fields exist.
* `json_importer` uploads catalog elements from `Исходные/clients_catalog_elements_final.json` into amoCRM using a bearer token.
* `leads_generator.py` wipes and repopulates the rental pipeline with synthetic leads spanning multiple rental stages.

All loaders expect a `.env` file with amoCRM credentials (`AMO_TOKEN`, `AMO_SUBDOMAIN`) and depend on `requests` plus `python-dotenv`.

## Local development & testing

1. **Environment variables** – Provide a `.env` file in the repository root (mounted to `/opt/airflow/.env` in Docker) containing PostgreSQL (`PG_HOST`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`) and amoCRM secrets (`AMO_TOKEN`, `AMO_SUBDOMAIN`).
2. **Python dependencies** – Airflow images already bundle the required libraries (`psycopg2-binary`, `requests`, `python-dotenv`). For standalone scripts, install them locally via `pip install -r requirements.txt` if present, or install the packages manually.
3. **Dry runs** – Use `airflow dags test <dag_id> <execution_date>` inside the Docker compose environment to validate DAG logic without waiting for the scheduler.
4. **SQL execution** – DAG tasks invoke SQL scripts from the mounted `/opt/airflow/sql/...` paths; ensure these files are available inside the Airflow container.

## Data flow summary

1. **Extract** – amoCRM APIs provide leads, clients, cars and dictionaries. Extract tasks paginate through the REST endpoints with rate limiting (`time.sleep(0.15)`) and write JSON snapshots to temporary files.
2. **Load (STG)** – JSON snapshots are inserted into staging tables with `ON CONFLICT DO NOTHING` to achieve incremental loads.
3. **Transform (ODS/DDS)** – SQL merge scripts translate staging data into historical ODS tables and dimensional/fact DDS tables suitable for BI consumption.
4. **Monitor** – Airflow logs and task prints (e.g., `[extract] 42 leads`) are accessible under `/opt/airflow/logs`, mirrored locally under `Логи/` if mounted.

## Getting started

1. Clone the repository and open it in your Airflow deployment.
2. Copy `.env.example` (if available) or craft a new `.env` with the credentials listed above.
3. Launch the orchestration stack via `docker compose up -d` (compose file not included here, but the DAGs assume such a setup).
4. Verify connections by triggering `daily_amocrm_load_leads` for a backfill date, then allow downstream DAGs to propagate the data to ODS and DDS layers.

For further implementation details or to contribute new DAGs, follow the conventions in `Даги инструкция` to keep the platform consistent.
