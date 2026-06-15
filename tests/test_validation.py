"""
Unit tests for the validation checks.

These don't require Airflow or network access — they exercise the pure-Python
validation functions directly. Run with:

    pytest -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make ./scripts importable when running pytest from the repo root.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from validate import (  # noqa: E402
    ValidationError,
    check_high_low_consistency,
    check_no_duplicate_rows,
    check_no_missing_in_key_columns,
    check_numeric_dtypes,
    check_positive_prices,
    check_required_columns,
    validate_financial_data,
)


def _good_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ticker": "AAPL", "date": "2024-01-02", "open": 185.0, "high": 187.0,
             "low": 184.0, "close": 186.0, "volume": 50_000_000},
            {"ticker": "AAPL", "date": "2024-01-03", "open": 186.5, "high": 188.0,
             "low": 185.0, "close": 187.5, "volume": 48_000_000},
            {"ticker": "MSFT", "date": "2024-01-02", "open": 372.0, "high": 376.0,
             "low": 370.0, "close": 375.0, "volume": 22_000_000},
        ]
    )


# ---- individual checks ------------------------------------------------------

def test_required_columns_pass():
    assert check_required_columns(_good_frame()).passed


def test_required_columns_fail():
    df = _good_frame().drop(columns=["close"])
    res = check_required_columns(df)
    assert not res.passed
    assert "close" in res.metrics["missing"]


def test_missing_key_columns():
    df = _good_frame()
    df.loc[0, "close"] = None
    assert not check_no_missing_in_key_columns(df).passed


def test_positive_prices_detects_negative():
    df = _good_frame()
    df.loc[0, "open"] = -1.0
    assert not check_positive_prices(df).passed


def test_high_low_consistency_detects_inversion():
    df = _good_frame()
    df.loc[0, "low"] = 999.0  # low > high
    assert not check_high_low_consistency(df).passed


def test_no_duplicates_detects_duplicate():
    df = pd.concat([_good_frame(), _good_frame().iloc[[0]]], ignore_index=True)
    assert not check_no_duplicate_rows(df).passed


def test_numeric_dtypes_detects_string_price():
    df = _good_frame()
    df["close"] = df["close"].astype(str)
    assert not check_numeric_dtypes(df).passed


# ---- end-to-end against the CSV path ---------------------------------------

def test_validate_financial_data_passes(tmp_path):
    p = tmp_path / "good.csv"
    _good_frame().to_csv(p, index=False)
    report = validate_financial_data(str(p))
    assert report["all_passed"] is True
    assert report["row_count"] == 3


def test_validate_financial_data_raises_on_bad(tmp_path):
    p = tmp_path / "bad.csv"
    bad = _good_frame()
    bad.loc[0, "close"] = None
    bad.to_csv(p, index=False)
    with pytest.raises(ValidationError):
        validate_financial_data(str(p))
