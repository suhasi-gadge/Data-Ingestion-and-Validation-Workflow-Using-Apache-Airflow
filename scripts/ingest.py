"""
Data ingestion module.

Downloads historical daily price data for one or more tickers and writes it
to a CSV staging file. Uses yfinance as the primary source and falls back to
the Stooq public CSV endpoint if yfinance is unavailable (e.g., offline).
"""

from __future__ import annotations

import io
import logging
import os
from datetime import date, datetime, timedelta
from typing import Iterable

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Tickers to ingest. Override via the AIRFLOW_TICKERS environment variable
# (comma-separated, e.g. "AAPL,MSFT,GOOG").
DEFAULT_TICKERS: tuple[str, ...] = ("AAPL", "MSFT", "GOOG", "SPY")

# Columns we always expect in the staged CSV.
EXPECTED_COLUMNS: tuple[str, ...] = (
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


def _tickers_from_env() -> list[str]:
    raw = os.getenv("AIRFLOW_TICKERS", "")
    if raw.strip():
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    return list(DEFAULT_TICKERS)


def _normalize_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Coerce a per-ticker frame to the canonical schema."""
    df = df.copy()
    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]

    # Some sources return an index named "Date" rather than a column.
    if "date" not in df.columns:
        df = df.reset_index().rename(columns={"Date": "date", "index": "date"})
        df.columns = [str(c).lower() for c in df.columns]

    df["ticker"] = ticker.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)

    keep = [c for c in EXPECTED_COLUMNS if c in df.columns]
    df = df[keep]

    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _fetch_yfinance(tickers: Iterable[str], start: date, end: date) -> pd.DataFrame:
    import yfinance as yf  # imported lazily so the script still loads without it

    frames: list[pd.DataFrame] = []
    for ticker in tickers:
        logger.info("yfinance: downloading %s (%s → %s)", ticker, start, end)
        raw = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            progress=False,
            auto_adjust=False,
        )
        if raw is None or raw.empty:
            logger.warning("yfinance returned empty frame for %s", ticker)
            continue
        # Newer yfinance versions return a MultiIndex on columns even for a
        # single ticker — flatten it.
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        frames.append(_normalize_frame(raw.reset_index(), ticker))

    if not frames:
        raise RuntimeError("yfinance returned no data for any requested ticker")
    return pd.concat(frames, ignore_index=True)


def _fetch_stooq(tickers: Iterable[str], start: date, end: date) -> pd.DataFrame:
    """Fallback source: Stooq publishes free daily CSVs at stooq.com/q/d/l."""
    frames: list[pd.DataFrame] = []
    for ticker in tickers:
        # Stooq uses the .us suffix for US tickers and lowercase symbols.
        stooq_symbol = f"{ticker.lower()}.us"
        url = (
            "https://stooq.com/q/d/l/"
            f"?s={stooq_symbol}"
            f"&d1={start.strftime('%Y%m%d')}"
            f"&d2={end.strftime('%Y%m%d')}"
            "&i=d"
        )
        logger.info("stooq: GET %s", url)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or text.lower().startswith("no data"):
            logger.warning("stooq returned no data for %s", ticker)
            continue
        df = pd.read_csv(io.StringIO(text))
        if df.empty:
            continue
        frames.append(_normalize_frame(df, ticker))

    if not frames:
        raise RuntimeError("stooq returned no data for any requested ticker")
    return pd.concat(frames, ignore_index=True)


def ingest_financial_data(
    output_path: str,
    tickers: list[str] | None = None,
    lookback_days: int = 365,
) -> str:
    """
    Download daily price data for `tickers` and write it as CSV to `output_path`.

    Returns the absolute path of the staged file. Raises if neither source yields data.
    """
    tickers = tickers or _tickers_from_env()
    end = date.today()
    start = end - timedelta(days=lookback_days)

    logger.info("Ingesting tickers=%s lookback_days=%d", tickers, lookback_days)

    df: pd.DataFrame
    try:
        df = _fetch_yfinance(tickers, start, end)
        source = "yfinance"
    except Exception as exc:  # pragma: no cover - network dependent
        logger.warning("yfinance ingest failed (%s); falling back to stooq", exc)
        df = _fetch_stooq(tickers, start, end)
        source = "stooq"

    # Ensure a stable column order even if a source omits one.
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[list(EXPECTED_COLUMNS)]

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

    logger.info(
        "Wrote %d rows from %s to %s (tickers=%s)",
        len(df),
        source,
        output_path,
        sorted(df["ticker"].unique().tolist()),
    )
    return os.path.abspath(output_path)


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s | %(message)s")
    out = ingest_financial_data(
        output_path=os.path.join(
            os.path.dirname(__file__), "..", "data", f"staging_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        )
    )
    print(f"Staged: {out}")
