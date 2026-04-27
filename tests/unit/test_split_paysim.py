"""Unit tests for scripts/split_paysim.py.

These tests follow the principle from the README §6: pure-function tests
on small, in-memory data. No file I/O except into a tmp_path fixture.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make the scripts/ directory importable from tests/
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from split_paysim import (  # noqa: E402
    OVERFLOW_DAY_LABEL,
    REQUIRED_COLUMNS,
    assign_day,
    split,
    validate_schema,
)


# ---------------------------------------------------------------
# assign_day
# ---------------------------------------------------------------
class TestAssignDay:
    @pytest.mark.parametrize(
        ("step", "expected"),
        [
            (1, "01"),
            (24, "01"),
            (25, "02"),
            (48, "02"),
            (49, "03"),
            (720, "30"),     # last hour of day 30
            (721, OVERFLOW_DAY_LABEL),
            (743, OVERFLOW_DAY_LABEL),
        ],
    )
    def test_known_steps(self, step: int, expected: str) -> None:
        assert assign_day(step) == expected

    def test_zero_or_negative_step_raises(self) -> None:
        with pytest.raises(ValueError, match="step must be >= 1"):
            assign_day(0)
        with pytest.raises(ValueError, match="step must be >= 1"):
            assign_day(-5)

    def test_day_labels_are_zero_padded(self) -> None:
        # All daily labels should be exactly 2 chars (or the overflow label)
        for step in (1, 25, 217, 720):
            label = assign_day(step)
            assert len(label) == 2 and label.isdigit()


# ---------------------------------------------------------------
# validate_schema
# ---------------------------------------------------------------
class TestValidateSchema:
    def test_full_schema_passes(self) -> None:
        df = pd.DataFrame(columns=list(REQUIRED_COLUMNS))
        validate_schema(df)  # should not raise

    def test_missing_column_raises(self) -> None:
        cols = list(REQUIRED_COLUMNS)
        cols.remove("step")
        df = pd.DataFrame(columns=cols)
        with pytest.raises(ValueError, match="missing required PaySim columns"):
            validate_schema(df)

    def test_extra_columns_are_allowed(self) -> None:
        cols = list(REQUIRED_COLUMNS) + ["some_future_column"]
        df = pd.DataFrame(columns=cols)
        validate_schema(df)  # should not raise — additive schema is fine


# ---------------------------------------------------------------
# split (end-to-end)
# ---------------------------------------------------------------
def _make_synthetic_paysim(rows_per_step: dict[int, int]) -> pd.DataFrame:
    """Build a small PaySim-shaped DataFrame for tests.

    rows_per_step: e.g. {1: 3, 25: 2, 721: 1} → 3 rows at step 1,
    2 rows at step 25, 1 row at step 721.
    """
    rows = []
    txn = 0
    for step, count in rows_per_step.items():
        for _ in range(count):
            txn += 1
            rows.append({
                "step": step,
                "type": "PAYMENT",
                "amount": 100.0,
                "nameOrig": f"C{txn:09d}",
                "oldbalanceOrg": 1000.0,
                "newbalanceOrig": 900.0,
                "nameDest": f"M{txn:09d}",
                "oldbalanceDest": 0.0,
                "newbalanceDest": 0.0,
                "isFraud": 0,
                "isFlaggedFraud": 0,
            })
    return pd.DataFrame(rows)


class TestSplit:
    def test_writes_one_file_per_day_seen(self, tmp_path: Path) -> None:
        # 3 rows in day 1, 2 in day 2, 1 in overflow
        df = _make_synthetic_paysim({1: 3, 25: 2, 721: 1})
        input_csv = tmp_path / "paysim.csv"
        df.to_csv(input_csv, index=False)

        output_dir = tmp_path / "out"
        counts = split(input_csv, output_dir)

        assert (output_dir / "day_01.csv").exists()
        assert (output_dir / "day_02.csv").exists()
        assert (output_dir / f"day_{OVERFLOW_DAY_LABEL}.csv").exists()

        assert counts == {"01": 3, "02": 2, OVERFLOW_DAY_LABEL: 1}

    def test_row_counts_preserved(self, tmp_path: Path) -> None:
        df = _make_synthetic_paysim({1: 5, 100: 7, 720: 2})
        input_csv = tmp_path / "paysim.csv"
        df.to_csv(input_csv, index=False)

        counts = split(input_csv, tmp_path / "out")
        assert sum(counts.values()) == len(df)

    def test_output_has_no_helper_columns(self, tmp_path: Path) -> None:
        # The internal `_day` column must not leak into output files
        df = _make_synthetic_paysim({1: 2})
        input_csv = tmp_path / "paysim.csv"
        df.to_csv(input_csv, index=False)

        split(input_csv, tmp_path / "out")
        out = pd.read_csv(tmp_path / "out" / "day_01.csv")
        assert "_day" not in out.columns
        assert set(REQUIRED_COLUMNS).issubset(out.columns)

    def test_missing_input_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="Input CSV not found"):
            split(tmp_path / "does_not_exist.csv", tmp_path / "out")

    def test_creates_output_dir(self, tmp_path: Path) -> None:
        df = _make_synthetic_paysim({1: 1})
        input_csv = tmp_path / "paysim.csv"
        df.to_csv(input_csv, index=False)

        nested_output = tmp_path / "deeply" / "nested" / "dir"
        assert not nested_output.exists()

        split(input_csv, nested_output)
        assert nested_output.exists()
        assert (nested_output / "day_01.csv").exists()
