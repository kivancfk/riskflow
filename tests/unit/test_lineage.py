"""Unit tests for transformations/lineage.py.

These tests use a real local SparkSession on tiny in-memory DataFrames.
They are slower than truly pure-function tests (Spark startup is ~5s
per session) but the coverage they buy is real: the schema validator
and the column-addition logic are both exercised against PySpark's
actual semantics, not a mocked-out approximation.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from transformations.lineage import (
    LINEAGE_COLUMNS,
    PAYSIM_SCHEMA,
    add_lineage_columns,
    validate_paysim_schema,
)


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------
@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """One SparkSession shared across all tests in this module."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("riskflow-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def _sample_paysim_row() -> dict:
    """One PaySim-shaped record for building tiny test DataFrames."""
    return {
        "step": 1,
        "type": "PAYMENT",
        "amount": 100.0,
        "nameOrig": "C123",
        "oldbalanceOrg": 1000.0,
        "newbalanceOrig": 900.0,
        "nameDest": "M456",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }


# ---------------------------------------------------------------
# add_lineage_columns
# ---------------------------------------------------------------
class TestAddLineageColumns:
    def test_adds_three_lineage_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_paysim_row()], schema=PAYSIM_SCHEMA)

        result = add_lineage_columns(
            df,
            load_ts="2026-05-03T08:00:00Z",
            source_file="day_07.csv",
            run_id="manual__2026-05-03T08:00:00",
        )

        for col in LINEAGE_COLUMNS:
            assert col in result.columns, f"missing lineage column: {col}"

    def test_preserves_source_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_paysim_row()], schema=PAYSIM_SCHEMA)
        original_cols = set(df.columns)

        result = add_lineage_columns(
            df,
            load_ts="2026-05-03T08:00:00Z",
            source_file="day_07.csv",
            run_id="r1",
        )

        # Original columns survive
        assert original_cols.issubset(set(result.columns))
        # Row count is unchanged
        assert result.count() == df.count()

    def test_load_ts_becomes_timestamp_type(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_paysim_row()], schema=PAYSIM_SCHEMA)

        result = add_lineage_columns(
            df,
            load_ts="2026-05-03T08:00:00Z",
            source_file="day_07.csv",
            run_id="r1",
        )

        load_ts_field = result.schema["_load_ts"]
        assert isinstance(load_ts_field.dataType, T.TimestampType), (
            f"_load_ts should be TimestampType, got {load_ts_field.dataType}"
        )

    def test_lineage_values_are_correct(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_paysim_row()], schema=PAYSIM_SCHEMA)

        result = add_lineage_columns(
            df,
            load_ts="2026-05-03T08:00:00Z",
            source_file="day_07.csv",
            run_id="manual__abc",
        ).collect()

        row = result[0]
        assert row["_source_file"] == "day_07.csv"
        assert row["_run_id"] == "manual__abc"
        # Timestamp comparison via string roundtrip is robust to TZ adjustment
        assert "2026-05-03" in str(row["_load_ts"])

    @pytest.mark.parametrize("kwarg", ["load_ts", "source_file", "run_id"])
    def test_empty_string_arg_raises(
        self, spark: SparkSession, kwarg: str,
    ) -> None:
        df = spark.createDataFrame([_sample_paysim_row()], schema=PAYSIM_SCHEMA)
        kwargs = {
            "load_ts": "2026-05-03T08:00:00Z",
            "source_file": "day_07.csv",
            "run_id": "r1",
        }
        kwargs[kwarg] = ""

        with pytest.raises(ValueError, match="must be"):
            add_lineage_columns(df, **kwargs)


# ---------------------------------------------------------------
# validate_paysim_schema
# ---------------------------------------------------------------
class TestValidatePaysimSchema:
    def test_correct_schema_passes(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_paysim_row()], schema=PAYSIM_SCHEMA)
        validate_paysim_schema(df)  # should not raise

    def test_missing_column_raises(self, spark: SparkSession) -> None:
        # Build a DataFrame with `step` removed
        row = _sample_paysim_row()
        del row["step"]
        partial_schema = T.StructType([
            f for f in PAYSIM_SCHEMA.fields if f.name != "step"
        ])
        df = spark.createDataFrame([row], schema=partial_schema)

        with pytest.raises(ValueError, match="missing columns.*step"):
            validate_paysim_schema(df)

    def test_extra_column_raises(self, spark: SparkSession) -> None:
        # Build a DataFrame with one extra column
        row = _sample_paysim_row()
        row["future_column"] = "x"
        extended_schema = T.StructType(
            list(PAYSIM_SCHEMA.fields) + [
                T.StructField("future_column", T.StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame([row], schema=extended_schema)

        with pytest.raises(ValueError, match="unexpected columns.*future_column"):
            validate_paysim_schema(df)

    def test_both_missing_and_extra_listed(self, spark: SparkSession) -> None:
        # Remove one, add one: error message should mention both
        row = _sample_paysim_row()
        del row["step"]
        row["new_col"] = "x"

        modified_schema = T.StructType(
            [f for f in PAYSIM_SCHEMA.fields if f.name != "step"]
            + [T.StructField("new_col", T.StringType(), nullable=True)]
        )
        df = spark.createDataFrame([row], schema=modified_schema)

        with pytest.raises(ValueError) as exc_info:
            validate_paysim_schema(df)

        msg = str(exc_info.value)
        assert "step" in msg
        assert "new_col" in msg
