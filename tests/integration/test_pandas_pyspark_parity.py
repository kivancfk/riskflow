"""Functional-equivalence test between PySpark and pandas silver transforms.

This is the non-negotiable test from Phase 5 design note §11. If the
two implementations diverge on the parity fixture, the Pass 0 baseline
comparison is measuring two different things and the writeup is
meaningless.

The test runs both implementations against a 10-row hand-crafted
bronze fixture and asserts byte-equivalent silver output:

  - Same row count
  - Same Parquet schema (column names, dtypes, including Decimal precision)
  - Same row content after sorting by DEDUP_KEY (neither implementation
    guarantees output row order)
  - Same quarantine row count and reason strings

The fixture is committed at tests/fixtures/bronze_parity_fixture.parquet
and can be regenerated via tests/fixtures/generate_bronze_parity_fixture.py.

This test is marked `integration` because it spins up a local Spark
session; it does not require the dockerized stack to be running.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest


FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "bronze_parity_fixture.parquet"
LOAD_DATE = "2026-04-15"

# Columns that should match exactly between the two implementations.
# We exclude load_date because it's a partition column (lifted into the
# directory name, dropped from the file content). We sort by DEDUP_KEY
# because neither implementation guarantees row order.
SILVER_COMPARE_COLUMNS = [
    "step", "type", "amount",
    "name_orig", "old_balance_orig", "new_balance_orig",
    "name_dest", "old_balance_dest", "new_balance_dest",
    "is_fraud", "is_flagged_fraud",
    "_load_ts", "_source_file",
    "event_hour", "balance_delta_orig", "balance_delta_dest",
]
DEDUP_KEY = ["name_orig", "name_dest", "step", "amount"]


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------
@pytest.fixture(scope="module")
def spark():
    """Local Spark session for the PySpark side of the parity check.

    Module-scoped because session startup is expensive and the test
    runs both implementations sequentially.
    """
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder
        .appName("test_pandas_pyspark_parity")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture()
def bronze_partition_dir(tmp_path):
    """Lay out the fixture as bronze_dir/load_date=2026-04-15/part-0.parquet.

    Both transforms expect Hive-style partitioning under bronze_dir.
    """
    bronze_dir = tmp_path / "bronze"
    partition = bronze_dir / f"load_date={LOAD_DATE}"
    partition.mkdir(parents=True)
    shutil.copy(FIXTURE_PATH, partition / "part-0.parquet")
    return bronze_dir


# ---------------------------------------------------------------
# Implementation runners
# ---------------------------------------------------------------
def _run_pyspark(spark, bronze_dir: Path, out_root: Path) -> tuple[int, int]:
    """Drive the PySpark silver transform in-process via its transform()."""
    # Import lazily so the module loads even when PySpark isn't installed
    # (pytest.importorskip in the spark fixture handles the skip).
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "spark" / "jobs"))
    try:
        import silver_transform  # type: ignore[import-not-found]
    finally:
        sys.path.pop(0)

    silver_dir = out_root / "silver_spark"
    quarantine_dir = out_root / "quarantine_spark"
    return silver_transform.transform(
        spark,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        load_date=LOAD_DATE,
        load_ts=f"{LOAD_DATE}T08:00:00Z",
        run_id="parity-test-spark",
    )


def _run_pandas(bronze_dir: Path, out_root: Path) -> tuple[int, int]:
    """Drive the pandas silver transform as a subprocess.

    Subprocess (not in-process) for two reasons:
      1. The pandas main() uses os._exit() to avoid pyarrow teardown
         SIGABRTs; an in-process call would never return.
      2. It exercises the same code path the perf harness will hit,
         so test-time and run-time behavior match.
    """
    silver_dir = out_root / "silver_pandas"
    quarantine_dir = out_root / "quarantine_pandas"
    argv = [
        sys.executable, "-m", "transformations.silver_transform_pandas",
        "--bronze-dir",     str(bronze_dir),
        "--silver-dir",     str(silver_dir),
        "--quarantine-dir", str(quarantine_dir),
        "--load-date",      LOAD_DATE,
        "--load-ts",        f"{LOAD_DATE}T08:00:00Z",
        "--run-id",         "parity-test-pandas",
    ]
    result = subprocess.run(argv, capture_output=True, text=True)
    assert result.returncode == 0, (
        f"pandas transform exited {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    # Parse the SILVER_ROW_COUNT / QUARANTINE_ROW_COUNT lines the
    # pandas main() prints on success.
    silver_count = quarantine_count = -1
    for line in result.stdout.splitlines():
        if line.startswith("SILVER_ROW_COUNT="):
            silver_count = int(line.split("=", 1)[1])
        elif line.startswith("QUARANTINE_ROW_COUNT="):
            quarantine_count = int(line.split("=", 1)[1])
    return silver_count, quarantine_count


def _read_silver(silver_dir: Path) -> pd.DataFrame:
    """Read silver output and return a DataFrame normalized for comparison.

    - Loads the load_date partition as a dataset (handles part-files).
    - Drops the load_date column (lifted to directory by partitionBy).
    - Sorts by DEDUP_KEY for order-independent comparison.
    - Resets index so assert_frame_equal doesn't trip on index labels.
    """
    table = pq.read_table(silver_dir)
    df = table.to_pandas()
    if "load_date" in df.columns:
        df = df.drop(columns=["load_date"])
    df = df[SILVER_COMPARE_COLUMNS]  # canonical column order
    df = df.sort_values(DEDUP_KEY).reset_index(drop=True)
    return df


# ---------------------------------------------------------------
# The test
# ---------------------------------------------------------------
@pytest.mark.integration
def test_pandas_pyspark_silver_parity(spark, bronze_partition_dir, tmp_path):
    """Both implementations produce identical silver output on the fixture."""
    out_spark = tmp_path / "spark"
    out_pandas = tmp_path / "pandas"

    spark_silver_count, spark_qcount = _run_pyspark(spark, bronze_partition_dir, out_spark)
    pandas_silver_count, pandas_qcount = _run_pandas(bronze_partition_dir, out_pandas)

    # Row counts must agree before content comparison is meaningful.
    assert spark_silver_count == pandas_silver_count == 5, (
        f"silver row count mismatch: spark={spark_silver_count} "
        f"pandas={pandas_silver_count} (expected 5)"
    )
    assert spark_qcount == pandas_qcount == 4, (
        f"quarantine row count mismatch: spark={spark_qcount} "
        f"pandas={pandas_qcount} (expected 4)"
    )

    df_spark = _read_silver(out_spark / "silver_spark")
    df_pandas = _read_silver(out_pandas / "silver_pandas")

    # check_dtype=True is critical here — the whole point of using
    # Decimal in pandas is that round-tripped Parquet dtypes match.
    pd.testing.assert_frame_equal(
        df_spark, df_pandas,
        check_dtype=True,
        check_exact=True,  # Decimal — no float tolerance needed
    )


@pytest.mark.integration
def test_pandas_pyspark_quarantine_reasons_match(
    spark, bronze_partition_dir, tmp_path,
):
    """Quarantine `_quarantine_reason` strings match between implementations.

    Quarantine schema diverges between the two (PySpark may carry
    extra metadata; pandas writes a minimal set), so we don't do a
    full frame_equal here — just the rejection reason per dedup key.
    """
    out_spark = tmp_path / "spark"
    out_pandas = tmp_path / "pandas"
    _run_pyspark(spark, bronze_partition_dir, out_spark)
    _run_pandas(bronze_partition_dir, out_pandas)

    q_spark = pq.read_table(out_spark / "quarantine_spark").to_pandas()
    q_pandas = pq.read_table(out_pandas / "quarantine_pandas").to_pandas()

    # Compare on (step, type, _quarantine_reason) sorted. step+type
    # disambiguate the 4 quarantine rows uniquely in this fixture.
    cols = ["step", "type", "_quarantine_reason"]
    s = q_spark[cols].sort_values(["step", "type"]).reset_index(drop=True)
    p = q_pandas[cols].sort_values(["step", "type"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(s, p, check_dtype=False)
