"""Integration tests for spark/jobs/silver_transform.py (Phase 2 v1).

End-to-end runs of transform() on a real Parquet fixture, verifying
that silver outputs use snake_case is_fraud / is_flagged_fraud.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

# Path resolution
_repo_root = Path(__file__).resolve().parents[2]
_candidate_paths = (
    _repo_root / "spark" / "jobs",
    Path("/opt/airflow/spark_jobs"),
)
for _path in _candidate_paths:
    if (_path / "silver_transform.py").exists():
        sys.path.insert(0, str(_path))
        break

from silver_transform import transform  # noqa: E402

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("riskflow-silver-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def bronze_fixture(tmp_path: Path, spark: SparkSession) -> Path:
    from pyspark.sql import functions as F

    schema = T.StructType([
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
        T.StructField("load_date",       T.DateType(),    nullable=True),
    ])

    rows = [
        # clean rows
        (1, "PAYMENT", 100.0, "C001", 1000.0, 900.0, "M001", 0.0, 100.0, 0, 0,
         None, "day_07.csv", "run-1", None),
        (2, "TRANSFER", 500.0, "C002", 500.0, 0.0, "C003", 0.0, 500.0, 1, 0,
         None, "day_07.csv", "run-1", None),
        # duplicate
        (1, "PAYMENT", 100.0, "C001", 1000.0, 900.0, "M001", 0.0, 100.0, 0, 0,
         None, "day_07.csv", "run-2", None),
    ]

    df = spark.createDataFrame(rows, schema=schema)
    bronze_dir = tmp_path / "bronze"
    (
        df.withColumn("load_date", F.to_date(F.lit("2026-04-07")))
        .write.mode("overwrite")
        .partitionBy("load_date")
        .parquet(str(bronze_dir))
    )
    return bronze_dir


class TestTransformEndToEnd:
    def test_silver_count_after_dedup(
        self, spark: SparkSession, bronze_fixture: Path, tmp_path: Path,
    ) -> None:
        silver_dir = tmp_path / "silver"
        quarantine_dir = tmp_path / "quarantine"

        silver_count, quarantine_count = transform(
            spark,
            bronze_dir=bronze_fixture, silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            load_date="2026-04-07",
            load_ts="2026-04-07T08:00:00Z",
            run_id="test-run",
        )

        # 3 bronze rows → 1 duplicate removed → 2 silver rows
        assert silver_count == 2
        assert quarantine_count == 0

    def test_silver_uses_snake_case_fraud_columns(
        self, spark: SparkSession, bronze_fixture: Path, tmp_path: Path,
    ) -> None:
        """End-to-end: silver Parquet has is_fraud + is_flagged_fraud."""
        silver_dir = tmp_path / "silver"
        quarantine_dir = tmp_path / "quarantine"

        transform(
            spark, bronze_dir=bronze_fixture, silver_dir=silver_dir,
            quarantine_dir=quarantine_dir, load_date="2026-04-07",
            load_ts="2026-04-07T08:00:00Z", run_id="test-run",
        )

        df = spark.read.parquet(str(silver_dir))
        assert "is_fraud" in df.columns
        assert "is_flagged_fraud" in df.columns
        assert "isFraud" not in df.columns
        assert "isFlaggedFraud" not in df.columns

    def test_silver_has_derived_columns(
        self, spark: SparkSession, bronze_fixture: Path, tmp_path: Path,
    ) -> None:
        silver_dir = tmp_path / "silver"
        quarantine_dir = tmp_path / "quarantine"

        transform(
            spark, bronze_dir=bronze_fixture, silver_dir=silver_dir,
            quarantine_dir=quarantine_dir, load_date="2026-04-07",
            load_ts="2026-04-07T08:00:00Z", run_id="test-run",
        )

        df = spark.read.parquet(str(silver_dir))
        for col in ("event_hour", "balance_delta_orig", "balance_delta_dest"):
            assert col in df.columns

    def test_idempotent_rerun(
        self, spark: SparkSession, bronze_fixture: Path, tmp_path: Path,
    ) -> None:
        silver_dir = tmp_path / "silver"
        quarantine_dir = tmp_path / "quarantine"

        transform(
            spark, bronze_dir=bronze_fixture, silver_dir=silver_dir,
            quarantine_dir=quarantine_dir, load_date="2026-04-07",
            load_ts="2026-04-07T08:00:00Z", run_id="run-1",
        )
        transform(
            spark, bronze_dir=bronze_fixture, silver_dir=silver_dir,
            quarantine_dir=quarantine_dir, load_date="2026-04-07",
            load_ts="2026-04-07T09:00:00Z", run_id="run-2",
        )

        df = spark.read.parquet(str(silver_dir))
        assert df.count() == 2  # not 4 — overwrite worked
