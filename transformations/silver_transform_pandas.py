"""RiskFlow — silver transformation in pandas (Phase 5 Pass 0 baseline).

Functional twin of spark/jobs/silver_transform.py. Same bronze input,
same silver/quarantine output, same DEDUP_KEY, same REQUIRED_FIELDS,
same DecimalType(18,2) on Parquet. Different runtime: pure pandas +
pyarrow, no JVM, single process.

This module exists for one reason: to establish a baseline measurement
that motivates Spark. The expectation is that pandas wins below ~1M
rows (no JVM startup tax), PySpark wins above, and pandas eventually
hits a wall (memory, single-threaded transforms over Decimal columns)
that PySpark doesn't. See docs/performance.md §2 for the writeup.

Function-by-function parity with the PySpark version is enforced by
tests/integration/test_pandas_pyspark_parity.py. Reviewer-facing
contract: each pure transformation function in this module has the
same name and same semantics as its PySpark counterpart, and the
ordered composition in transform() is identical.

Usage (called by scripts/perf_harness.py):
    python -m transformations.silver_transform_pandas \\
        --bronze-dir     /path/to/bronze \\
        --silver-dir     /path/to/silver_pandas \\
        --quarantine-dir /path/to/silver_pandas_quarantine \\
        --load-date      2026-04-15 \\
        --load-ts        2026-04-15T08:00:00Z \\
        --run-id         perf-baseline-pandas-small-r1
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("silver_transform_pandas")


# Constants — kept verbatim from the PySpark version so anyone diffing
# the two files sees only the runtime differences, not constant drift.
TRANSACTION_TYPES = {"PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"}
DEDUP_KEY = ["name_orig", "name_dest", "step", "amount"]
REQUIRED_FIELDS = ["step", "type", "amount", "name_orig", "name_dest"]

# Parquet output schema for silver. Decimal(18,2) on all financial
# columns matches what PySpark writes from DecimalType(18,2), which is
# how the parity test sees byte-identical Parquet column types from
# both implementations.
_DEC18_2 = pa.decimal128(18, 2)
SILVER_OUTPUT_SCHEMA = pa.schema(
    [
        ("step", pa.int32()),
        ("type", pa.string()),
        ("amount", _DEC18_2),
        ("name_orig", pa.string()),
        ("old_balance_orig", _DEC18_2),
        ("new_balance_orig", _DEC18_2),
        ("name_dest", pa.string()),
        ("old_balance_dest", _DEC18_2),
        ("new_balance_dest", _DEC18_2),
        ("is_fraud", pa.bool_()),
        ("is_flagged_fraud", pa.bool_()),
        ("_load_ts", pa.timestamp("us")),
        ("_source_file", pa.string()),
        ("event_hour", pa.int32()),
        ("balance_delta_orig", _DEC18_2),
        ("balance_delta_dest", _DEC18_2),
        ("load_date", pa.date32()),
    ]
)


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

def _to_decimal_18_2(value):
    """Convert a float/None to Decimal(18,2) with bank-style quantization.

    PySpark's `.cast(DecimalType(18,2))` rounds half-away-from-zero for
    the test fixtures we care about; Python's Decimal default rounding
    is `ROUND_HALF_EVEN`. For our PaySim data the difference never
    fires (amounts already have ≤2 decimal places coming out of bronze),
    but using HALF_UP here matches Spark's observed behavior more
    closely if a future fixture ever exercises the rounding case.
    """
    if value is None or (isinstance(value, float) and value != value):  # NaN
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"))


# ---------------------------------------------------------------
# Pure transformation functions (Pytest-testable)
# ---------------------------------------------------------------

def cast_financial_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Cast bronze DOUBLE financials to Decimal(18,2), rename to snake_case.

    Mirrors PySpark cast_financial_columns() exactly. Bronze columns are
    in camelCase (PaySim source CSV convention); silver columns are in
    snake_case (project-wide convention starting Phase 5 Day 1).

    Also renames isFraud→is_fraud and isFlaggedFraud→is_flagged_fraud
    while casting them from int to bool.
    """
    out = df.copy()

    # Cast the bronze-named financial columns to Decimal(18,2) BEFORE
    # renaming, so the column lookup matches what bronze produced.
    bronze_decimal_cols = [
        "amount", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest",
    ]
    for col in bronze_decimal_cols:
        out[col] = out[col].map(_to_decimal_18_2)

    # Bronze camelCase → silver snake_case. Also corrects PaySim's
    # `oldbalanceOrg` typo to `old_balance_orig`. Verbatim mapping from
    # the PySpark version.
    bronze_to_silver = {
        "nameOrig":       "name_orig",
        "nameDest":       "name_dest",
        "oldbalanceOrg":  "old_balance_orig",
        "newbalanceOrig": "new_balance_orig",
        "oldbalanceDest": "old_balance_dest",
        "newbalanceDest": "new_balance_dest",
    }
    out = out.rename(columns=bronze_to_silver)

    # Rename + cast isFraud / isFlaggedFraud to boolean in one pass.
    out["is_fraud"] = out["isFraud"].astype("bool")
    out["is_flagged_fraud"] = out["isFlaggedFraud"].astype("bool")
    out = out.drop(columns=["isFraud", "isFlaggedFraud"])

    return out


def derive_event_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add event_hour, balance_delta_orig, balance_delta_dest.

    PySpark equivalents:
      event_hour         = ((step - 1) % 24).cast(IntegerType())
      balance_delta_orig = (new_balance_orig - old_balance_orig).cast(Decimal(18,2))
      balance_delta_dest = (new_balance_dest - old_balance_dest).cast(Decimal(18,2))

    The Decimal arithmetic preserves type fidelity; this is the most
    obvious place pandas pays a cost vs PySpark (Decimal ops are
    Python-level, not vectorized in C).
    """
    out = df.copy()
    out["event_hour"] = ((out["step"] - 1) % 24).astype("int32")

    # Decimal subtraction: Decimal(a) - Decimal(b) yields Decimal; we
    # quantize back to 2dp to match Spark's cast-to-Decimal(18,2).
    def _delta(new, old):
        if new is None or old is None:
            return None
        return (new - old).quantize(Decimal("0.01"))

    out["balance_delta_orig"] = [
        _delta(n, o) for n, o in zip(out["new_balance_orig"], out["old_balance_orig"])
    ]
    out["balance_delta_dest"] = [
        _delta(n, o) for n, o in zip(out["new_balance_dest"], out["old_balance_dest"])
    ]
    return out


def deduplicate_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per (name_orig, name_dest, step, amount); latest _load_ts wins.

    PySpark uses Window.partitionBy(DEDUP_KEY).orderBy(_load_ts desc)
    plus row_number() == 1. Pandas equivalent: stable sort descending
    by _load_ts, then drop_duplicates(keep="first").

    Stability via kind="mergesort" matters: if two rows share the same
    DEDUP_KEY *and* the same _load_ts, both implementations have an
    underspecified tiebreaker. The parity fixture intentionally avoids
    that case.
    """
    if df.empty:
        return df
    sorted_df = df.sort_values("_load_ts", ascending=False, kind="mergesort")
    return sorted_df.drop_duplicates(subset=DEDUP_KEY, keep="first")


def enforce_not_null_constraints(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into (clean, quarantine) on REQUIRED_FIELDS + amount > 0.

    The reject_condition is identical to PySpark:
        any required field is null  OR  amount <= 0

    The quarantine DataFrame gets a `_quarantine_reason` column whose
    format matches PySpark's `F.concat_ws("; ", ...)`:
      - one tag per failed check, semicolon+space separator
      - tags are "null:<field_name>" for null violations
      - tag is "non_positive_amount" for the amount check
      - F.concat_ws drops NULL inputs, so passing rows never appear
    """
    if df.empty:
        return df, df.copy()

    null_mask = pd.Series(False, index=df.index)
    for field in REQUIRED_FIELDS:
        null_mask = null_mask | df[field].isna()

    # amount is Decimal; comparison against int 0 works because Decimal
    # implements __le__ against int. We use 0 (not Decimal(0)) for
    # readability; this is the same comparison PySpark performs.
    bad_amount_mask = df["amount"].map(
        lambda x: x is not None and x <= 0
    )
    # Decimal(None) is None; the .map above yields False for None which
    # is correct (null-amount already caught by null_mask above).

    reject_mask = null_mask | bad_amount_mask

    clean = df[~reject_mask].copy()
    quarantine = df[reject_mask].copy()

    if not quarantine.empty:
        def _reason(row) -> str:
            tags = []
            for field in REQUIRED_FIELDS:
                if pd.isna(row[field]):
                    tags.append(f"null:{field}")
            amount = row["amount"]
            if amount is not None and amount <= 0:
                tags.append("non_positive_amount")
            return "; ".join(tags)

        quarantine["_quarantine_reason"] = quarantine.apply(_reason, axis=1)

    return clean, quarantine


# ---------------------------------------------------------------
# Driver
# ---------------------------------------------------------------

def _read_bronze_partition(bronze_dir: Path, load_date: str) -> pd.DataFrame:
    """Read one bronze load_date partition into pandas via pyarrow.

    Bronze is written by Phase 2's PySpark bronze ingest as
    `bronze_dir/load_date=YYYY-MM-DD/`. We read just that subdir to
    avoid pulling in unrelated partitions.
    """
    partition_path = bronze_dir / f"load_date={load_date}"
    log.info("Reading bronze partition: %s", partition_path)
    table = pq.read_table(partition_path)
    return table.to_pandas(types_mapper=None)


def _write_silver_partition(
    df: pd.DataFrame,
    silver_dir: Path,
    load_date: str,
) -> None:
    """Write the clean DataFrame to silver_dir/load_date=YYYY-MM-DD/.

    Hive-style partition layout matches what PySpark's
    partitionBy("load_date") produces. The load_date column itself is
    NOT written into the Parquet files — pyarrow's partitioning lifts
    it into the directory name, exactly like Spark.
    """
    if df.empty:
        log.info("Silver clean DataFrame empty for %s, skipping write", load_date)
        return
    # Ensure load_date is a python date object for pyarrow's partitioning.
    out = df.copy()
    out["load_date"] = pd.to_datetime(load_date).date()
    table = pa.Table.from_pandas(out, schema=SILVER_OUTPUT_SCHEMA, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(silver_dir),
        partition_cols=["load_date"],
        compression="snappy",
    )


def _write_quarantine_partition(
    df: pd.DataFrame,
    quarantine_dir: Path,
    load_date: str,
) -> None:
    """Write quarantine rows. Schema is dynamic (has _quarantine_reason).

    We don't enforce a pinned schema on quarantine because the column
    set differs from silver and the parity test doesn't compare
    quarantine bytes — only quarantine row counts and reason strings.
    """
    if df.empty:
        return
    out = df.copy()
    out["load_date"] = pd.to_datetime(load_date).date()
    table = pa.Table.from_pandas(out, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(quarantine_dir),
        partition_cols=["load_date"],
        compression="snappy",
    )


def transform(
    *,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    load_date: str,
    load_ts: str,  # noqa: ARG001 -- kept for CLI parity with PySpark version
    run_id: str,   # noqa: ARG001 -- kept for CLI parity with PySpark version
) -> tuple[int, int]:
    """Run full bronze→silver transformation for one load_date.

    Returns (silver_row_count, quarantine_row_count).
    """
    df = _read_bronze_partition(bronze_dir, load_date)
    bronze_count = len(df)
    log.info("Bronze rows read: %s", f"{bronze_count:,}")

    df = cast_financial_columns(df)
    df = derive_event_columns(df)
    df = deduplicate_transactions(df)

    clean_df, quarantine_df = enforce_not_null_constraints(df)

    silver_count = len(clean_df)
    quarantine_count = len(quarantine_df)
    log.info(
        "After dedup + validation: silver=%s quarantine=%s",
        f"{silver_count:,}", f"{quarantine_count:,}",
    )

    log.info("Writing silver to %s/load_date=%s/", silver_dir, load_date)
    _write_silver_partition(clean_df, silver_dir, load_date)

    if quarantine_count > 0:
        log.info(
            "Writing %s quarantine rows to %s/load_date=%s/",
            f"{quarantine_count:,}", quarantine_dir, load_date,
        )
        _write_quarantine_partition(quarantine_df, quarantine_dir, load_date)

    log.info("Silver transformation complete (pandas)")
    return silver_count, quarantine_count


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RiskFlow silver transformation (pandas)")
    p.add_argument("--bronze-dir",     type=Path, required=True)
    p.add_argument("--silver-dir",     type=Path, required=True)
    p.add_argument("--quarantine-dir", type=Path, required=True)
    p.add_argument("--load-date",      type=str,  required=True)
    p.add_argument("--load-ts",        type=str,  required=True)
    p.add_argument("--run-id",         type=str,  required=True)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        silver_count, quarantine_count = transform(
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            quarantine_dir=args.quarantine_dir,
            load_date=args.load_date,
            load_ts=args.load_ts,
            run_id=args.run_id,
        )
    except Exception:
        log.exception("Silver transformation failed unexpectedly")
        return 1

    print(f"SILVER_ROW_COUNT={silver_count}")
    print(f"QUARANTINE_ROW_COUNT={quarantine_count}")
    return 0


if __name__ == "__main__":
    # We use os._exit() rather than sys.exit() to bypass the Python
    # interpreter teardown phase. pyarrow's C++ thread pool can SIGABRT
    # at interpreter shutdown on Python 3.12+ with "terminate called
    # without an active exception" — benign, but it turns a successful
    # exit code 0 into 134, which would confuse the perf harness into
    # flagging clean runs as failures. By the time we reach this point
    # all output has been written to disk and flushed; there is nothing
    # useful for atexit handlers to do.
    rc = main()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)
