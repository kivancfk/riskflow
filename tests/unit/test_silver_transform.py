"""Unit tests for spark/jobs/silver_transform.py (Phase 2 v1).

Verifies the snake_case rename: silver outputs `is_fraud` and
`is_flagged_fraud`, dropping the bronze-layer `isFraud` and
`isFlaggedFraud` columns.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

# Path resolution — works locally and inside container
_repo_root = Path(__file__).resolve().parents[2]
_candidate_paths = (
    _repo_root / "spark" / "jobs",
    Path("/opt/airflow/spark_jobs"),
)
for _path in _candidate_paths:
    if (_path / "silver_transform.py").exists():
        sys.path.insert(0, str(_path))
        break

from silver_transform import (  # noqa: E402
    DEDUP_KEY,
    REQUIRED_FIELDS,
    cast_financial_columns,
    deduplicate_transactions,
    derive_event_columns,
    enforce_not_null_constraints,
)


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------
@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("riskflow-silver-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def _bronze_schema() -> T.StructType:
    """Schema matching what bronze_ingest.py writes — camelCase."""
    return T.StructType([
        T.StructField("step",            T.IntegerType(), nullable=False),
        T.StructField("type",            T.StringType(),  nullable=False),
        T.StructField("amount",          T.DoubleType(),  nullable=False),
        T.StructField("nameOrig",        T.StringType(),  nullable=False),
        T.StructField("oldbalanceOrg",   T.DoubleType(),  nullable=True),
        T.StructField("newbalanceOrig",  T.DoubleType(),  nullable=True),
        T.StructField("nameDest",        T.StringType(),  nullable=False),
        T.StructField("oldbalanceDest",  T.DoubleType(),  nullable=True),
        T.StructField("newbalanceDest",  T.DoubleType(),  nullable=True),
        T.StructField("isFraud",         T.IntegerType(), nullable=False),
        T.StructField("isFlaggedFraud",  T.IntegerType(), nullable=False),
        T.StructField("_load_ts",        T.TimestampType(), nullable=True),
        T.StructField("_source_file",    T.StringType(),  nullable=True),
        T.StructField("_run_id",         T.StringType(),  nullable=True),
    ])


def _sample_row(**overrides) -> dict:
    base = {
        "step": 1, "type": "PAYMENT", "amount": 100.0,
        "nameOrig": "C001", "oldbalanceOrg": 1000.0, "newbalanceOrig": 900.0,
        "nameDest": "M001", "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
        "isFraud": 0, "isFlaggedFraud": 0,
        "_load_ts": None, "_source_file": "day_07.csv", "_run_id": "run-1",
    }
    base.update(overrides)
    return base


def _silver_intermediate_schema() -> T.StructType:
    """Schema as it looks AFTER cast_financial_columns has run.

    The six PaySim camelCase columns are renamed to snake_case (with the
    `Org` -> `orig` typo corrected), and isFraud/isFlaggedFraud are dropped
    in favor of boolean is_fraud/is_flagged_fraud. Used as input to the
    functions that run downstream of cast_financial_columns:
    derive_event_columns, deduplicate_transactions,
    enforce_not_null_constraints.
    """
    return T.StructType([
        T.StructField("step",               T.IntegerType(), nullable=False),
        T.StructField("type",               T.StringType(),  nullable=False),
        T.StructField("amount",             T.DoubleType(),  nullable=False),
        T.StructField("name_orig",          T.StringType(),  nullable=False),
        T.StructField("old_balance_orig",   T.DoubleType(),  nullable=True),
        T.StructField("new_balance_orig",   T.DoubleType(),  nullable=True),
        T.StructField("name_dest",          T.StringType(),  nullable=False),
        T.StructField("old_balance_dest",   T.DoubleType(),  nullable=True),
        T.StructField("new_balance_dest",   T.DoubleType(),  nullable=True),
        T.StructField("is_fraud",           T.BooleanType(), nullable=False),
        T.StructField("is_flagged_fraud",   T.BooleanType(), nullable=False),
        T.StructField("_load_ts",           T.TimestampType(), nullable=True),
        T.StructField("_source_file",       T.StringType(),  nullable=True),
        T.StructField("_run_id",            T.StringType(),  nullable=True),
    ])


def _silver_intermediate_row(**overrides) -> dict:
    """Row dict matching _silver_intermediate_schema()."""
    base = {
        "step": 1, "type": "PAYMENT", "amount": 100.0,
        "name_orig": "C001", "old_balance_orig": 1000.0, "new_balance_orig": 900.0,
        "name_dest": "M001", "old_balance_dest": 0.0, "new_balance_dest": 0.0,
        "is_fraud": False, "is_flagged_fraud": False,
        "_load_ts": None, "_source_file": "day_07.csv", "_run_id": "run-1",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------
# cast_financial_columns
# ---------------------------------------------------------------
class TestCastFinancialColumns:
    def test_amount_cast_to_decimal(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_row()], schema=_bronze_schema())
        result = cast_financial_columns(df)
        assert isinstance(result.schema["amount"].dataType, T.DecimalType)

    def test_renames_isfraud_to_snake_case(self, spark: SparkSession) -> None:
        """The silver naming convention: is_fraud + is_flagged_fraud."""
        df = spark.createDataFrame([_sample_row()], schema=_bronze_schema())
        result = cast_financial_columns(df)
        assert "is_fraud" in result.columns
        assert "is_flagged_fraud" in result.columns
        # Old camelCase columns dropped
        assert "isFraud" not in result.columns
        assert "isFlaggedFraud" not in result.columns

    def test_is_fraud_cast_to_boolean(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_row()], schema=_bronze_schema())
        result = cast_financial_columns(df)
        assert isinstance(result.schema["is_fraud"].dataType, T.BooleanType)
        assert isinstance(result.schema["is_flagged_fraud"].dataType, T.BooleanType)

    def test_fraud_flag_values_correct(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_sample_row(isFraud=1, isFlaggedFraud=0)],
            schema=_bronze_schema(),
        )
        row = cast_financial_columns(df).collect()[0]
        assert row["is_fraud"] is True
        assert row["is_flagged_fraud"] is False

    def test_row_count_unchanged(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_sample_row(), _sample_row(step=2)],
            schema=_bronze_schema(),
        )
        assert cast_financial_columns(df).count() == 2

    def test_all_decimal_cols_cast(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_sample_row()], schema=_bronze_schema())
        result = cast_financial_columns(df)
        for col in (
            "amount", "old_balance_orig", "new_balance_orig",
            "old_balance_dest", "new_balance_dest",
        ):
            assert isinstance(result.schema[col].dataType, T.DecimalType), (
                f"{col} should be DecimalType"
            )


# ---------------------------------------------------------------
# derive_event_columns
# ---------------------------------------------------------------
class TestDeriveEventColumns:
    def test_three_new_columns_added(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_silver_intermediate_row()], schema=_silver_intermediate_schema())
        result = derive_event_columns(df)
        for col in ("event_hour", "balance_delta_orig", "balance_delta_dest"):
            assert col in result.columns

    @pytest.mark.parametrize("step,expected_hour", [
        (1, 0), (24, 23), (25, 0), (48, 23), (49, 0),
    ])
    def test_event_hour_formula(
        self, spark: SparkSession, step: int, expected_hour: int,
    ) -> None:
        df = spark.createDataFrame(
            [_silver_intermediate_row(step=step)], schema=_silver_intermediate_schema(),
        )
        row = derive_event_columns(df).collect()[0]
        assert row["event_hour"] == expected_hour

    def test_balance_delta_orig_formula(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_silver_intermediate_row(old_balance_orig=1000.0, new_balance_orig=900.0)],
            schema=_silver_intermediate_schema(),
        )
        row = derive_event_columns(df).collect()[0]
        assert float(row["balance_delta_orig"]) == pytest.approx(-100.0)

    def test_balance_delta_dest_formula(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_silver_intermediate_row(old_balance_dest=0.0, new_balance_dest=100.0)],
            schema=_silver_intermediate_schema(),
        )
        row = derive_event_columns(df).collect()[0]
        assert float(row["balance_delta_dest"]) == pytest.approx(100.0)


# ---------------------------------------------------------------
# deduplicate_transactions
# ---------------------------------------------------------------
class TestDeduplicateTransactions:
    def test_exact_duplicate_removed(self, spark: SparkSession) -> None:
        row = _silver_intermediate_row()
        df = spark.createDataFrame([row, row], schema=_silver_intermediate_schema())
        result = deduplicate_transactions(df)
        assert result.count() == 1

    def test_different_transactions_kept(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_silver_intermediate_row(step=1), _silver_intermediate_row(step=2)],
            schema=_silver_intermediate_schema(),
        )
        assert deduplicate_transactions(df).count() == 2

    def test_no_dedup_rank_column_in_output(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_silver_intermediate_row()], schema=_silver_intermediate_schema())
        result = deduplicate_transactions(df)
        assert "_dedup_rank" not in result.columns


# ---------------------------------------------------------------
# enforce_not_null_constraints
# ---------------------------------------------------------------
class TestEnforceNotNullConstraints:
    def test_clean_row_goes_to_clean(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_silver_intermediate_row()], schema=_silver_intermediate_schema())
        clean, quarantine = enforce_not_null_constraints(df)
        assert clean.count() == 1
        assert quarantine.count() == 0

    def test_null_required_field_goes_to_quarantine(
        self, spark: SparkSession,
    ) -> None:
        nullable = T.StructType([
            f if f.name != "step"
            else T.StructField("step", T.IntegerType(), nullable=True)
            for f in _silver_intermediate_schema().fields
        ])
        row = _silver_intermediate_row()
        row["step"] = None
        df = spark.createDataFrame([row], schema=nullable)
        clean, quarantine = enforce_not_null_constraints(df)
        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_non_positive_amount_quarantined(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [_silver_intermediate_row(amount=0.0)], schema=_silver_intermediate_schema(),
        )
        clean, quarantine = enforce_not_null_constraints(df)
        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_quarantine_reason_column_present(self, spark: SparkSession) -> None:
        nullable = T.StructType([
            f if f.name != "step"
            else T.StructField("step", T.IntegerType(), nullable=True)
            for f in _silver_intermediate_schema().fields
        ])
        row = _silver_intermediate_row()
        row["step"] = None
        df = spark.createDataFrame([row], schema=nullable)
        _, quarantine = enforce_not_null_constraints(df)
        assert "_quarantine_reason" in quarantine.columns

    def test_mixed_rows_split_correctly(self, spark: SparkSession) -> None:
        nullable = T.StructType([
            f if f.name != "step"
            else T.StructField("step", T.IntegerType(), nullable=True)
            for f in _silver_intermediate_schema().fields
        ])
        good = _silver_intermediate_row()
        bad = _silver_intermediate_row(step=None)
        df = spark.createDataFrame([good, bad], schema=nullable)
        clean, quarantine = enforce_not_null_constraints(df)
        assert clean.count() == 1
        assert quarantine.count() == 1
