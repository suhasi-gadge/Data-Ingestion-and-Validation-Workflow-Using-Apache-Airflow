"""
Data validation module.

Runs a battery of data-quality checks against the staged CSV. Each check
returns a dict so we can aggregate them into a single report that gets
surfaced in the Airflow logs and (optionally) pushed via XCom.

A single failing check causes `validate_financial_data` to raise
`ValidationError`, which marks the Airflow task — and therefore the DAG run —
as failed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: tuple[str, ...] = (
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

NUMERIC_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close", "volume")


class ValidationError(RuntimeError):
    """Raised when one or more validation checks fail."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
            "metrics": self.metrics,
        }


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_required_columns(df: pd.DataFrame) -> CheckResult:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return CheckResult(
        name="required_columns_present",
        passed=not missing,
        detail=f"missing columns: {missing}" if missing else "all required columns present",
        metrics={"missing": missing, "found": list(df.columns)},
    )


def check_non_empty(df: pd.DataFrame) -> CheckResult:
    return CheckResult(
        name="non_empty",
        passed=len(df) > 0,
        detail=f"row count = {len(df)}",
        metrics={"row_count": int(len(df))},
    )


def check_no_missing_in_key_columns(df: pd.DataFrame) -> CheckResult:
    """No nulls allowed in ticker, date, or close."""
    key_cols = ["ticker", "date", "close"]
    present = [c for c in key_cols if c in df.columns]
    null_counts = {c: int(df[c].isna().sum()) for c in present}
    total_nulls = sum(null_counts.values())
    return CheckResult(
        name="no_missing_in_key_columns",
        passed=total_nulls == 0,
        detail=f"null counts: {null_counts}",
        metrics=null_counts,
    )


def check_numeric_dtypes(df: pd.DataFrame) -> CheckResult:
    """OHLC and volume columns must be numeric (parseable as floats)."""
    bad: dict[str, str] = {}
    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            bad[col] = str(df[col].dtype)
    return CheckResult(
        name="numeric_dtypes",
        passed=not bad,
        detail=f"non-numeric columns: {bad}" if bad else "all price/volume columns numeric",
        metrics={"non_numeric": bad},
    )


def check_positive_prices(df: pd.DataFrame) -> CheckResult:
    """Prices must be strictly positive; volume must be non-negative."""
    issues: dict[str, int] = {}
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            issues[col] = int((df[col] <= 0).sum())
    if "volume" in df.columns:
        issues["volume_negative"] = int((df["volume"] < 0).sum())
    total = sum(issues.values())
    return CheckResult(
        name="positive_prices_and_nonneg_volume",
        passed=total == 0,
        detail=f"bad rows per column: {issues}",
        metrics=issues,
    )


def check_high_low_consistency(df: pd.DataFrame) -> CheckResult:
    """For every row: low <= open/close <= high."""
    needed = {"high", "low", "open", "close"}
    if not needed.issubset(df.columns):
        return CheckResult(
            name="high_low_consistency",
            passed=True,
            detail="skipped: not all OHLC columns present",
        )
    clean = df.dropna(subset=list(needed))
    bad = clean[
        (clean["low"] > clean["high"])
        | (clean["open"] < clean["low"])
        | (clean["open"] > clean["high"])
        | (clean["close"] < clean["low"])
        | (clean["close"] > clean["high"])
    ]
    return CheckResult(
        name="high_low_consistency",
        passed=len(bad) == 0,
        detail=f"{len(bad)} rows violate low<=o,c<=high",
        metrics={"violations": int(len(bad))},
    )


def check_date_parsable(df: pd.DataFrame) -> CheckResult:
    if "date" not in df.columns:
        return CheckResult(name="date_parsable", passed=False, detail="no date column")
    parsed = pd.to_datetime(df["date"], errors="coerce")
    bad = int(parsed.isna().sum())
    return CheckResult(
        name="date_parsable",
        passed=bad == 0,
        detail=f"{bad} unparsable date values",
        metrics={"unparsable": bad},
    )


def check_no_duplicate_rows(df: pd.DataFrame) -> CheckResult:
    if not {"ticker", "date"}.issubset(df.columns):
        return CheckResult(name="no_duplicate_rows", passed=True, detail="skipped")
    dup = int(df.duplicated(subset=["ticker", "date"]).sum())
    return CheckResult(
        name="no_duplicate_rows",
        passed=dup == 0,
        detail=f"{dup} duplicate (ticker, date) rows",
        metrics={"duplicates": dup},
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

CHECKS: tuple[Callable[[pd.DataFrame], CheckResult], ...] = (
    check_required_columns,
    check_non_empty,
    check_no_missing_in_key_columns,
    check_numeric_dtypes,
    check_positive_prices,
    check_high_low_consistency,
    check_date_parsable,
    check_no_duplicate_rows,
)


def validate_financial_data(staging_path: str) -> dict[str, Any]:
    """
    Run all checks on the staged CSV. Raises ValidationError on any failure.

    Returns a report dict suitable for logging or pushing through XCom.
    """
    logger.info("Reading staging file: %s", staging_path)
    df = pd.read_csv(staging_path)

    results: list[CheckResult] = []
    for check in CHECKS:
        result = check(df)
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        logger.info("[%s] %s — %s", status, result.name, result.detail)

    report = {
        "staging_path": staging_path,
        "row_count": int(len(df)),
        "checks": [r.to_dict() for r in results],
        "all_passed": all(r.passed for r in results),
    }

    failed = [r.name for r in results if not r.passed]
    if failed:
        raise ValidationError(
            f"Validation failed for {staging_path}. Failing checks: {failed}"
        )

    logger.info("All %d checks passed for %s", len(results), staging_path)
    return report


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s | %(message)s")
    path = sys.argv[1] if len(sys.argv) > 1 else "data/staging.csv"
    print(validate_financial_data(path))
