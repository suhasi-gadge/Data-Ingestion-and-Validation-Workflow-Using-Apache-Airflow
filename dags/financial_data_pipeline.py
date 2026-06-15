"""
financial_data_pipeline
=======================

A small but realistic ingest → validate → load pipeline for public financial
data.

Tasks
-----
1. **ingest_data**           – Download recent daily OHLCV bars for a list of
                               tickers and write them to a CSV staging file.
2. **validate_data**         – Run a battery of data-quality checks. Raises
                               (and so fails the DAG) if any check fails.
3. **transform_and_load**    – Compute daily returns and upsert into SQLite.
4. **report**                – Log a small human-readable summary.

The DAG passes file paths between tasks via XCom (the staging CSV is also
written to a deterministic per-run path so it can be inspected after the run).

A second DAG, `financial_data_pipeline_invalid_demo`, is included so reviewers
can see the validation step fail on demand without having to find broken data
in the wild.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the project's ./scripts directory importable inside the Airflow
# container. AIRFLOW_HOME defaults to /opt/airflow when running under the
# official image; ../scripts relative to this DAG file works locally too.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from ingest import ingest_financial_data  # noqa: E402
from transform_load import transform_and_load  # noqa: E402
from validate import validate_financial_data  # noqa: E402

log = logging.getLogger(__name__)

# Data directory inside the container. The docker-compose mounts ./data here.
DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data"))
DB_PATH = DATA_DIR / "financial.db"


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _staging_path_for_run(run_id: str) -> str:
    safe = run_id.replace(":", "-").replace("+", "_")
    return str(DATA_DIR / f"staging_{safe}.csv")


def task_ingest(**context) -> str:
    out = _staging_path_for_run(context["run_id"])
    path = ingest_financial_data(output_path=out)
    log.info("Ingested data staged at %s", path)
    return path  # pushed to XCom as return_value


def task_validate(**context) -> dict:
    staging_path = context["ti"].xcom_pull(task_ids="ingest_data")
    if not staging_path:
        raise RuntimeError("No staging path received from ingest_data")
    report = validate_financial_data(staging_path)
    log.info("Validation report: %s", report)
    return report


def task_transform_and_load(**context) -> dict:
    staging_path = context["ti"].xcom_pull(task_ids="ingest_data")
    summary = transform_and_load(staging_path=staging_path, db_path=str(DB_PATH))
    log.info("Load summary: %s", summary)
    return summary


def task_report(**context) -> None:
    summary = context["ti"].xcom_pull(task_ids="transform_and_load")
    validation = context["ti"].xcom_pull(task_ids="validate_data")
    log.info("=" * 60)
    log.info("PIPELINE RUN SUMMARY")
    log.info("=" * 60)
    log.info("Validation passed: %s", validation.get("all_passed"))
    log.info("Rows validated:    %s", validation.get("row_count"))
    log.info("Rows written:      %s", summary.get("rows_written"))
    log.info("Rows in table:     %s", summary.get("rows_total_in_table"))
    log.info("Per ticker:        %s", summary.get("rows_per_ticker"))
    log.info("Database:          %s", summary.get("db_path"))
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# DAG: the real pipeline
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="financial_data_pipeline",
    description="Ingest, validate, and load public daily stock prices into SQLite.",
    default_args=default_args,
    schedule="0 6 * * 1-5",  # 06:00 UTC, weekdays
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "ingest", "validate", "sqlite"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=task_ingest,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate,
    )

    load = PythonOperator(
        task_id="transform_and_load",
        python_callable=task_transform_and_load,
    )

    report = PythonOperator(
        task_id="report",
        python_callable=task_report,
    )

    ingest >> validate >> load >> report


# ---------------------------------------------------------------------------
# DAG: deliberately-bad-data demo, for reviewers
# ---------------------------------------------------------------------------

def task_make_bad_data(**context) -> str:
    """Write a CSV with several intentional data-quality problems."""
    out = _staging_path_for_run(context["run_id"]).replace(
        "staging_", "staging_INVALID_"
    )
    bad = pd.DataFrame(
        [
            # missing close, missing date
            {"ticker": "AAPL", "date": "2024-01-02", "open": 185.0, "high": 186.0,
             "low": 184.0, "close": None, "volume": 50_000_000},
            {"ticker": "AAPL", "date": None,         "open": 187.0, "high": 188.0,
             "low": 186.0, "close": 187.5, "volume": 49_000_000},
            # negative price
            {"ticker": "MSFT", "date": "2024-01-02", "open": -10.0, "high": 380.0,
             "low": 370.0, "close": 375.0, "volume": 22_000_000},
            # low > high (impossible)
            {"ticker": "GOOG", "date": "2024-01-02", "open": 140.0, "high": 100.0,
             "low": 150.0, "close": 145.0, "volume": 18_000_000},
            # duplicate (ticker, date)
            {"ticker": "AAPL", "date": "2024-01-02", "open": 185.0, "high": 186.0,
             "low": 184.0, "close": 185.5, "volume": 50_000_000},
        ]
    )
    os.makedirs(os.path.dirname(out), exist_ok=True)
    bad.to_csv(out, index=False)
    log.info("Wrote intentionally-bad staging file: %s", out)
    return out


def task_validate_bad(**context) -> dict:
    staging_path = context["ti"].xcom_pull(task_ids="make_bad_data")
    return validate_financial_data(staging_path)


with DAG(
    dag_id="financial_data_pipeline_invalid_demo",
    description="Same pipeline against a deliberately-broken staging file — "
                "demonstrates that the validate step fails the DAG.",
    default_args=default_args,
    schedule=None,           # manual trigger only
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["finance", "validation-demo"],
) as demo_dag:

    make_bad = PythonOperator(
        task_id="make_bad_data",
        python_callable=task_make_bad_data,
    )
    validate_bad = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate_bad,
    )
    # `load` is intentionally downstream so reviewers can confirm it never runs.
    load_bad = PythonOperator(
        task_id="transform_and_load_should_not_run",
        python_callable=task_transform_and_load,
    )

    make_bad >> validate_bad >> load_bad
