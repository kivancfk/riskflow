"""Integration test for spark/jobs/bronze_ingest.py.

This is closer to the truth than the unit tests in test_lineage.py:
it actually invokes the Spark job, has it read a real CSV, validate
schema, add lineage columns, and write Parquet — then verifies the
Parquet was written correctly and re-running is idempotent.

Marked as `slow` because Spark startup + Parquet I/O takes ~10s.
Run via: make test-integration  (excluded from `make test-unit`).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

# Add spark/jobs to sys.path so we can import the script directly
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "jobs"))

from bronze_ingest import ingest  # noqa: E402

pytestmark = pytest.mark.slow


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------
@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("riskflow-integration-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def fixture_csv(tmp_path: Path) -> Path:
    """Write a tiny PaySim-shaped CSV to disk."""
    csv = tmp_path / "day_99.csv"
    csv.write_text(
        "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,"
        "nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n"
        "1,PAYMENT,100.0,C001,1000.0,900.0,M001,0.0,0.0,0,0\n"
        "2,TRANSFER,500.0,C002,500.0,0.0,C003,0.0,500.0,1,0\n"
        "3,CASH_OUT,200.0,C004,300.0,100.0,M005,0.0,0.0,0,0\n"
    )
    return csv


# ---------------------------------------------------------------
# End-to-end ingestion
# ---------------------------------------------------------------
class TestIngestEndToEnd:
    def test_writes_partitioned_parquet(
        self, spark: SparkSession, fixture_csv: Path, tmp_path: Path,
    ) -> None:
        output = tmp_path / "bronze"

        row_count = ingest(
            spark,
            input_csv=fixture_csv,
            output_dir=output,
            load_date="2026-05-03",
            load_ts="2026-05-03T08:00:00Z",
            run_id="test-run-1",
        )

        assert row_count == 3
        assert (output / "load_date=2026-05-03").is_dir()
        success_marker = output / "load_date=2026-05-03" / "_SUCCESS"
        assert success_marker.exists()

    def test_lineage_columns_are_present_and_populated(
        self, spark: SparkSession, fixture_csv: Path, tmp_path: Path,
    ) -> None:
        output = tmp_path / "bronze"

        ingest(
            spark,
            input_csv=fixture_csv,
            output_dir=output,
            load_date="2026-05-03",
            load_ts="2026-05-03T08:00:00Z",
            run_id="test-run-2",
        )

        readback = spark.read.parquet(str(output)).collect()
        assert len(readback) == 3
        for row in readback:
            assert row["_source_file"] == "day_99.csv"
            assert row["_run_id"] == "test-run-2"
            assert row["_load_ts"] is not None

    def test_idempotent_rerun_replaces_partition(
        self, spark: SparkSession, fixture_csv: Path, tmp_path: Path,
    ) -> None:
        """Re-running same load_date overwrites, doesn't append."""
        output = tmp_path / "bronze"

        # First run
        ingest(
            spark, input_csv=fixture_csv, output_dir=output,
            load_date="2026-05-03", load_ts="2026-05-03T08:00:00Z",
            run_id="run-1",
        )
        # Second run — same load_date, different run_id
        ingest(
            spark, input_csv=fixture_csv, output_dir=output,
            load_date="2026-05-03", load_ts="2026-05-03T09:00:00Z",
            run_id="run-2",
        )

        df = spark.read.parquet(str(output))
        assert df.count() == 3, "Re-running same partition must not duplicate rows"

        # And the rows now reflect run-2's lineage
        run_ids = {r["_run_id"] for r in df.collect()}
        assert run_ids == {"run-2"}

    def test_two_different_dates_coexist(
        self, spark: SparkSession, fixture_csv: Path, tmp_path: Path,
    ) -> None:
        """Different load_dates write to different partitions."""
        output = tmp_path / "bronze"

        ingest(
            spark, input_csv=fixture_csv, output_dir=output,
            load_date="2026-05-03", load_ts="2026-05-03T08:00:00Z",
            run_id="run-day3",
        )
        ingest(
            spark, input_csv=fixture_csv, output_dir=output,
            load_date="2026-05-04", load_ts="2026-05-04T08:00:00Z",
            run_id="run-day4",
        )

        assert (output / "load_date=2026-05-03").is_dir()
        assert (output / "load_date=2026-05-04").is_dir()

        df = spark.read.parquet(str(output))
        assert df.count() == 6  # 3 rows × 2 partitions

    def test_schema_mismatch_raises(
        self, spark: SparkSession, tmp_path: Path,
    ) -> None:
        """A CSV with the wrong columns must fail the ingest, not silently succeed."""
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("col1,col2\nx,y\n")

        with pytest.raises(Exception):
            ingest(
                spark,
                input_csv=bad_csv,
                output_dir=tmp_path / "bronze",
                load_date="2026-05-03",
                load_ts="2026-05-03T08:00:00Z",
                run_id="bad-run",
            )
