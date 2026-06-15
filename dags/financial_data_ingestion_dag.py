"""Airflow DAG for public financial data ingestion, validation, and SQLite loading."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Allows the DAG to import /opt/airflow/src/finance_pipeline.py inside Docker.
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from finance_pipeline import (  # noqa: E402
    PipelineConfig,
    download_stooq_csv,
    load_to_sqlite,
    simulate_invalid_csv,
    transform_stock_prices,
    validate_financial_data,
)

CONFIG = PipelineConfig()


def ingest_data(**context) -> str:
    """Task 1: Ingest public financial CSV data into local staging."""
    csv_path = download_stooq_csv(CONFIG.source_url, CONFIG.staging_csv)

    dag_run = context.get("dag_run")
    simulate_invalid = bool(dag_run and dag_run.conf and dag_run.conf.get("simulate_invalid"))
    if simulate_invalid:
        csv_path = simulate_invalid_csv(csv_path)

    return csv_path


def validate_data(**context) -> dict:
    """Task 2: Validate staged data; raise an exception to fail the DAG on bad data."""
    csv_path = context["ti"].xcom_pull(task_ids="ingest_public_financial_data")
    return validate_financial_data(csv_path, min_rows=CONFIG.min_rows)


def transform_and_load(**context) -> int:
    """Task 3: Transform validated staged data and load it to SQLite."""
    csv_path = context["ti"].xcom_pull(task_ids="ingest_public_financial_data")
    curated_csv = transform_stock_prices(csv_path, CONFIG.curated_csv, CONFIG.symbol)
    return load_to_sqlite(curated_csv, CONFIG.sqlite_db, CONFIG.target_table)


DEFAULT_ARGS = {
    "owner": "suhasi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="public_financial_data_ingestion_validation",
    description="Ingest Stooq stock prices, validate OHLCV data, and load valid rows to SQLite.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["finance", "data-ingestion", "validation", "sqlite"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_public_financial_data",
        python_callable=ingest_data,
    )

    validate_task = PythonOperator(
        task_id="validate_staged_financial_data",
        python_callable=validate_data,
    )

    load_task = PythonOperator(
        task_id="transform_and_load_to_sqlite",
        python_callable=transform_and_load,
    )

    ingest_task >> validate_task >> load_task
