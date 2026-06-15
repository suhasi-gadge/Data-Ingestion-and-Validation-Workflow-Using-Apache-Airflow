"""
Transform-and-load module.

Reads the staged (and now validated) CSV, applies light transformations, and
upserts the result into a SQLite database. The schema is created on first run.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

TABLE_NAME = "daily_prices"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ticker        TEXT    NOT NULL,
    date          TEXT    NOT NULL,
    open          REAL,
    high          REAL,
    low           REAL,
    close         REAL    NOT NULL,
    volume        INTEGER,
    daily_return  REAL,
    ingested_at   TEXT    NOT NULL,
    PRIMARY KEY (ticker, date)
);
"""

INDEX_DDL = (
    f"CREATE INDEX IF NOT EXISTS ix_{TABLE_NAME}_ticker ON {TABLE_NAME} (ticker);",
    f"CREATE INDEX IF NOT EXISTS ix_{TABLE_NAME}_date   ON {TABLE_NAME} (date);",
)


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Add a daily_return column and stamp an ingestion timestamp."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Simple per-ticker daily return.
    df["daily_return"] = (
        df.groupby("ticker")["close"]
        .pct_change()
        .round(6)
    )
    df["ingested_at"] = pd.Timestamp.utcnow().isoformat(timespec="seconds")
    return df


def _upsert(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Insert rows, replacing any existing (ticker, date) primary-key conflicts.

    Returns the number of rows written.
    """
    cols = [
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "ingested_at",
    ]
    placeholders = ", ".join("?" for _ in cols)
    sql = (
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        f"({', '.join(cols)}) VALUES ({placeholders})"
    )
    rows = list(df[cols].itertuples(index=False, name=None))
    # SQLite represents NaN poorly; convert to None.
    cleaned = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in rows
    ]
    conn.executemany(sql, cleaned)
    return len(cleaned)


def transform_and_load(
    staging_path: str,
    db_path: str,
) -> dict[str, Any]:
    """
    Transform the staged CSV and upsert it into SQLite.

    Returns a small summary dict (row count, table name, db path).
    """
    logger.info("Loading staged data from %s", staging_path)
    df = pd.read_csv(staging_path)
    df = _transform(df)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    logger.info("Connecting to SQLite at %s", db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(DDL)
        for idx_stmt in INDEX_DDL:
            conn.execute(idx_stmt)
        n = _upsert(conn, df)
        conn.commit()

        total = conn.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME}"
        ).fetchone()[0]
        per_ticker = dict(
            conn.execute(
                f"SELECT ticker, COUNT(*) FROM {TABLE_NAME} GROUP BY ticker"
            ).fetchall()
        )

    summary = {
        "table": TABLE_NAME,
        "db_path": os.path.abspath(db_path),
        "rows_written": n,
        "rows_total_in_table": int(total),
        "rows_per_ticker": per_ticker,
    }
    logger.info("Load summary: %s", summary)
    return summary


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s | %(message)s")
    staging = sys.argv[1] if len(sys.argv) > 1 else "data/staging.csv"
    db = sys.argv[2] if len(sys.argv) > 2 else "data/financial.db"
    print(transform_and_load(staging, db))
