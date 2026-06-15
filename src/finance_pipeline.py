"""Reusable ingestion, validation, and load utilities for the Airflow financial data DAG."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
NUMERIC_COLUMNS = ["open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration for the local financial data pipeline."""

    symbol: str = "aapl.us"
    source_url: str = "https://stooq.com/q/d/l/?s=aapl.us&i=d"
    staging_csv: str = "/opt/airflow/data/staging_aapl_daily.csv"
    curated_csv: str = "/opt/airflow/data/curated_aapl_daily.csv"
    sqlite_db: str = "/opt/airflow/data/finance_warehouse.db"
    target_table: str = "stock_prices_daily"
    min_rows: int = 100


def ensure_parent_dir(path: str | Path) -> None:
    """Create the parent folder for a path if it does not already exist."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def download_stooq_csv(source_url: str, output_path: str) -> str:
    """Download Stooq CSV data into a local staging file."""
    ensure_parent_dir(output_path)
    LOGGER.info("Downloading financial data from %s", source_url)

    response = requests.get(source_url, timeout=30)
    response.raise_for_status()

    text = response.text.strip()
    if not text or "Date," not in text.splitlines()[0]:
        raise ValueError(
            "The source response did not look like a Stooq OHLCV CSV. "
            "Check the symbol and source_url."
        )

    Path(output_path).write_text(text + "\n", encoding="utf-8")
    LOGGER.info("Wrote staged CSV to %s", output_path)
    return output_path


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize source column names to lower snake-case names used downstream."""
    df = df.copy()
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def load_staged_csv(path: str) -> pd.DataFrame:
    """Load and lightly normalize a staged CSV."""
    if not Path(path).exists():
        raise FileNotFoundError(f"Staged CSV not found: {path}")
    return normalize_columns(pd.read_csv(path))


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> list[str]:
    """Return validation errors for missing required columns."""
    missing = sorted(set(required_columns) - set(df.columns))
    return [f"Missing required columns: {missing}"] if missing else []


def validate_financial_data(csv_path: str, min_rows: int = 100) -> dict:
    """
    Validate staged OHLCV stock data.

    Raises:
        ValueError: when one or more validation checks fail. Raising an exception
        makes the Airflow task and DAG run fail.
    """
    df = load_staged_csv(csv_path)
    errors: list[str] = []

    errors.extend(validate_required_columns(df, REQUIRED_COLUMNS))
    if errors:
        raise ValueError("Validation failed: " + " | ".join(errors))

    if len(df) < min_rows:
        errors.append(f"Expected at least {min_rows} rows, found {len(df)}")

    # Date validation.
    parsed_dates = pd.to_datetime(df["date"], errors="coerce")
    invalid_date_count = int(parsed_dates.isna().sum())
    if invalid_date_count:
        errors.append(f"Found {invalid_date_count} invalid or missing date values")

    duplicate_dates = int(df["date"].duplicated().sum())
    if duplicate_dates:
        errors.append(f"Found {duplicate_dates} duplicate date values")

    # Null checks for business-critical fields.
    missing_counts = df[REQUIRED_COLUMNS].isna().sum()
    missing_counts = missing_counts[missing_counts > 0]
    if not missing_counts.empty:
        errors.append(f"Missing values detected: {missing_counts.to_dict()}")

    # Type consistency checks.
    numeric_df = df[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
    invalid_numeric_counts = numeric_df.isna().sum() - df[NUMERIC_COLUMNS].isna().sum()
    invalid_numeric_counts = invalid_numeric_counts[invalid_numeric_counts > 0]
    if not invalid_numeric_counts.empty:
        errors.append(f"Non-numeric values detected: {invalid_numeric_counts.to_dict()}")

    # Range and OHLC consistency checks.
    if (numeric_df["open"] <= 0).any() or (numeric_df["high"] <= 0).any() or (numeric_df["low"] <= 0).any() or (numeric_df["close"] <= 0).any():
        errors.append("Prices must be strictly positive")
    if (numeric_df["volume"] < 0).any():
        errors.append("Volume must be non-negative")
    if (numeric_df["high"] < numeric_df["low"]).any():
        errors.append("High price cannot be lower than low price")
    if ((numeric_df["open"] < numeric_df["low"]) | (numeric_df["open"] > numeric_df["high"])).any():
        errors.append("Open price must be between low and high")
    if ((numeric_df["close"] < numeric_df["low"]) | (numeric_df["close"] > numeric_df["high"])).any():
        errors.append("Close price must be between low and high")

    if errors:
        raise ValueError("Validation failed: " + " | ".join(errors))

    return {
        "status": "passed",
        "row_count": int(len(df)),
        "min_date": str(parsed_dates.min().date()),
        "max_date": str(parsed_dates.max().date()),
    }


def transform_stock_prices(staging_csv: str, curated_csv: str, symbol: str) -> str:
    """Transform staged OHLCV data into a curated analytics-ready CSV."""
    df = load_staged_csv(staging_csv)
    df["date"] = pd.to_datetime(df["date"], errors="raise").dt.date

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise")

    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    df.insert(0, "symbol", symbol.upper())
    df["daily_return"] = df["close"].pct_change()
    df["dollar_volume"] = df["close"] * df["volume"]
    df["loaded_at_utc"] = pd.Timestamp.utcnow().isoformat()

    ensure_parent_dir(curated_csv)
    df.to_csv(curated_csv, index=False)
    LOGGER.info("Wrote curated CSV to %s", curated_csv)
    return curated_csv


def load_to_sqlite(curated_csv: str, sqlite_db: str, table_name: str) -> int:
    """Load the curated CSV into a SQLite table, replacing existing data."""
    ensure_parent_dir(sqlite_db)
    df = pd.read_csv(curated_csv)

    with sqlite3.connect(sqlite_db) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_date "
            f"ON {table_name} (symbol, date)"
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    LOGGER.info("Loaded %s rows into SQLite table %s", row_count, table_name)
    return int(row_count)


def simulate_invalid_csv(csv_path: str) -> str:
    """
    Corrupt staged data intentionally for failure testing.

    The DAG can call this when dag_run.conf contains {"simulate_invalid": true}.
    """
    df = load_staged_csv(csv_path)
    if df.empty:
        raise ValueError("Cannot simulate invalid data because staged dataset is empty")

    df.loc[df.index[0], "close"] = None
    if "high" in df.columns and "low" in df.columns and len(df) > 1:
        df.loc[df.index[1], "high"] = -1
    df.to_csv(csv_path, index=False)
    LOGGER.info("Simulated invalid data in %s", csv_path)
    return csv_path
