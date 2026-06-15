from pathlib import Path

import pandas as pd
import pytest

from src.finance_pipeline import validate_financial_data


def test_valid_data_passes(tmp_path: Path):
    path = tmp_path / "valid.csv"
    pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=120, freq="D").strftime("%Y-%m-%d"),
            "Open": [100.0] * 120,
            "High": [110.0] * 120,
            "Low": [90.0] * 120,
            "Close": [105.0] * 120,
            "Volume": [1000000] * 120,
        }
    ).to_csv(path, index=False)

    result = validate_financial_data(str(path), min_rows=100)
    assert result["status"] == "passed"
    assert result["row_count"] == 120


def test_invalid_data_fails(tmp_path: Path):
    path = tmp_path / "invalid.csv"
    pd.DataFrame(
        {
            "Date": ["2024-01-01", "2024-01-02"],
            "Open": [100.0, 100.0],
            "High": [110.0, -1.0],
            "Low": [90.0, 90.0],
            "Close": [None, 105.0],
            "Volume": [1000000, 1000000],
        }
    ).to_csv(path, index=False)

    with pytest.raises(ValueError, match="Validation failed"):
        validate_financial_data(str(path), min_rows=1)
