"""RiskFlow — silver transformation (bronze Parquet → silver Parquet).

Phase 2 v1: filesystem-based GE workflow.

Reads one bronze load_date partition, casts types, derives new columns,
deduplicates, quarantines bad rows, renames isFraud/isFlaggedFraud to
snake_case (is_fraud, is_flagged_fraud) per the silver naming convention,
and writes Parquet to data/silver/.

Self-contained — no imports from a transformations/ module.

Usage (called by SparkSubmitOperator):
    spark-submit silver_transform.py \\
        --bronze-dir    /opt/airflow/data/bronze \\
        --silver-dir    /opt/airflow/data/silver \\
        --quarantine-dir /opt/airflow/data/silver_quarantine \\
        --load-date     2026-04-07 \\
        --load-ts       2026-04-07T08:00:00Z \\
        --run-id        manual__2026-04-07T08:00:00+00:00
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("silver_transform")


# Constants used by both the job and the test suite
TRANSACTION_TYPES = {"PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"}
DEDUP_KEY = ["name_orig", "name_dest", "step", "amount"]
REQUIRED_FIELDS = ["step", "type", "amount", "name_orig", "name_dest"]


# ---------------------------------------------------------------
# Pure transformation functions (Pytest-testable)
# ---------------------------------------------------------------

def cast_financial_columns(df: DataFrame) -> DataFrame:
    """Cast DOUBLE financial columns to DECIMAL(18,2).

    Renames isFraud → is_fraud and isFlaggedFraud → is_flagged_fraud
    while casting them from INT to BOOLEAN. The rename is the silver
    layer's snake_case convention; bronze keeps the original camelCase
    from PaySim's source CSV.
    """
    # Cast financial DOUBLE columns to DECIMAL(18,2) on the bronze (camelCase) names,
    # then rename to snake_case as part of the silver-layer convention.
    bronze_decimal_cols = [
        "amount", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest",
    ]
    result = df
    for col in bronze_decimal_cols:
        result = result.withColumn(col, F.col(col).cast(T.DecimalType(18, 2)))

    # Rename bronze camelCase -> silver snake_case (also corrects PaySim's
    # `oldbalanceOrg` typo to `old_balance_orig`).
    bronze_to_silver = {
        "nameOrig":       "name_orig",
        "nameDest":       "name_dest",
        "oldbalanceOrg":  "old_balance_orig",
        "newbalanceOrig": "new_balance_orig",
        "oldbalanceDest": "old_balance_dest",
        "newbalanceDest": "new_balance_dest",
    }
    for src, dst in bronze_to_silver.items():
        result = result.withColumnRenamed(src, dst)

    # Rename + cast isFraud / isFlaggedFraud in a single pass
    result = (
        result
        .withColumn("is_fraud",         F.col("isFraud").cast(T.BooleanType()))
        .withColumn("is_flagged_fraud", F.col("isFlaggedFraud").cast(T.BooleanType()))
        .drop("isFraud", "isFlaggedFraud")
    )
    return result


def derive_event_columns(df: DataFrame) -> DataFrame:
    """Add three derived columns used by gold layer aggregations.

    event_hour:         hour-of-day (0–23) from PaySim's step counter.
    balance_delta_orig: new_balance_orig - old_balance_orig.
    balance_delta_dest: new_balance_dest - old_balance_dest.
    """
    return (
        df
        .withColumn(
            "event_hour",
            ((F.col("step") - 1) % 24).cast(T.IntegerType()),
        )
        .withColumn(
            "balance_delta_orig",
            (F.col("new_balance_orig") - F.col("old_balance_orig"))
            .cast(T.DecimalType(18, 2)),
        )
        .withColumn(
            "balance_delta_dest",
            (F.col("new_balance_dest") - F.col("old_balance_dest"))
            .cast(T.DecimalType(18, 2)),
        )
    )


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """Remove duplicate rows using the natural transaction key.

    Bronze can contain re-ingested rows from idempotent re-runs.
    Silver must contain exactly one row per distinct transaction.
    Strategy: keep the row with the latest _load_ts among duplicates.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy(*DEDUP_KEY).orderBy(F.col("_load_ts").desc())

    return (
        df
        .withColumn("_dedup_rank", F.row_number().over(window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


def enforce_not_null_constraints(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split DataFrame into (clean, quarantine) based on required fields.

    Returns:
        (clean_df, quarantine_df) — two DataFrames, never None.
    """
    null_condition = F.lit(False)
    for field in REQUIRED_FIELDS:
        null_condition = null_condition | F.col(field).isNull()

    bad_amount = F.col("amount") <= 0
    reject_condition = null_condition | bad_amount

    clean_df = df.filter(~reject_condition)

    reason_expr = F.concat_ws(
        "; ",
        *[
            F.when(F.col(f).isNull(), F.lit(f"null:{f}"))
            for f in REQUIRED_FIELDS
        ],
        F.when(F.col("amount") <= 0, F.lit("non_positive_amount")),
    )

    quarantine_df = (
        df.filter(reject_condition)
        .withColumn("_quarantine_reason", reason_expr)
    )

    return clean_df, quarantine_df


# ---------------------------------------------------------------
# Driver
# ---------------------------------------------------------------
def transform(
    spark: SparkSession,
    *,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    load_date: str,
    load_ts: str,
    run_id: str,
) -> tuple[int, int]:
    """Run full bronze→silver transformation for one load_date.

    Returns:
        (silver_row_count, quarantine_row_count)
    """
    partition_path = f"{bronze_dir}/load_date={load_date}"
    log.info("Reading bronze partition: %s", partition_path)

    df = spark.read.parquet(partition_path)
    bronze_count = df.count()
    log.info("Bronze rows read: %s", f"{bronze_count:,}")

    df = cast_financial_columns(df)
    df = derive_event_columns(df)
    df = deduplicate_transactions(df)

    clean_df, quarantine_df = enforce_not_null_constraints(df)

    # Add silver-layer load_date partition column
    clean_df = clean_df.withColumn("load_date", F.to_date(F.lit(load_date)))
    quarantine_df = quarantine_df.withColumn(
        "load_date", F.to_date(F.lit(load_date))
    )

    silver_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    log.info(
        "After dedup + validation: silver=%s quarantine=%s",
        f"{silver_count:,}", f"{quarantine_count:,}",
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    log.info("Writing silver to %s/load_date=%s/", silver_dir, load_date)
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("load_date")
        .parquet(str(silver_dir))
    )

    if quarantine_count > 0:
        log.info(
            "Writing %s quarantine rows to %s/load_date=%s/",
            f"{quarantine_count:,}", quarantine_dir, load_date,
        )
        (
            quarantine_df.write
            .mode("overwrite")
            .partitionBy("load_date")
            .parquet(str(quarantine_dir))
        )

    log.info("Silver transformation complete")
    return silver_count, quarantine_count


def transform_multi(
    spark: SparkSession,
    *,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    load_dates: list[str],
    load_ts: str,
    run_id: str,
) -> tuple[int, int]:
    """Run bronze→silver across N load_dates in a single Spark session.

    Phase 5 Day 2 fix: the perf harness was previously invoking this
    script once per load_date, paying ~25-30s of JVM cold start + driver
    handshake on every invocation. For the Large scale (30 partitions)
    that was ~900s of pure overhead before any actual work happened.

    This function reads all N partitions in one `spark.read.parquet(...,
    basePath=...)` call. Spark infers `load_date` from the directory
    names (so we don't add a literal), parallelizes the scans across
    its executors, and writes back with `partitionBy("load_date")` to
    the same Hive-style layout.

    Semantics are otherwise identical to `transform()` — same cast,
    same derives, same dedup key, same null/range rejection rules.
    Single-date callers (the Airflow DAG, the parity test, unit tests)
    should keep using `transform()` for backward compatibility.

    Returns:
        (silver_row_count, quarantine_row_count) summed across dates.
    """
    if not load_dates:
        raise ValueError("transform_multi requires at least one load_date")

    # Build the explicit list of partition paths. We pass them
    # individually rather than using a glob so a typo in one date
    # surfaces as a clean "path not found" rather than silently
    # reading nothing.
    partition_paths = [
        f"{bronze_dir}/load_date={d}" for d in load_dates
    ]
    log.info(
        "Reading %d bronze partitions in one Spark session (basePath=%s)",
        len(load_dates), bronze_dir,
    )

    # basePath is what tells Spark "the partition columns live in the
    # directory names rooted here" — without it, Spark would not
    # hydrate `load_date` as a column when reading individual leaf
    # partition paths. With it, `load_date` is read directly from the
    # directory name per row, which is the source of truth for which
    # date each row belongs to.
    df = (
        spark.read
        .option("basePath", str(bronze_dir))
        .parquet(*partition_paths)
    )
    bronze_count = df.count()
    log.info("Bronze rows read across %d partitions: %s",
             len(load_dates), f"{bronze_count:,}")

    # We DO NOT drop and re-derive load_date here. Spark's partition
    # discovery already hydrated it correctly from the directory name.
    # An earlier draft re-derived load_date from _load_ts on the
    # theory that _load_ts and partition date are consistent in real
    # bronze — they are, but tying the partition column to data
    # semantics is fragile (a clock skew or a backfill could break
    # it) and re-derivation provides zero benefit when the right
    # value is already in hand from the directory structure.

    df = cast_financial_columns(df)
    df = derive_event_columns(df)
    df = deduplicate_transactions(df)

    clean_df, quarantine_df = enforce_not_null_constraints(df)

    silver_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    log.info(
        "After dedup + validation across %d partitions: silver=%s quarantine=%s",
        len(load_dates), f"{silver_count:,}", f"{quarantine_count:,}",
    )

    # Dynamic mode is critical here: with N partitions written from
    # the same DataFrame, we want overwrite semantics scoped to the
    # partitions actually present in the write, not "blow away the
    # whole silver_dir".
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    log.info("Writing silver to %s (partitioned by load_date)", silver_dir)
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("load_date")
        .parquet(str(silver_dir))
    )

    if quarantine_count > 0:
        log.info(
            "Writing %s quarantine rows to %s (partitioned by load_date)",
            f"{quarantine_count:,}", quarantine_dir,
        )
        (
            quarantine_df.write
            .mode("overwrite")
            .partitionBy("load_date")
            .parquet(str(quarantine_dir))
        )

    log.info("Silver multi-partition transformation complete")
    return silver_count, quarantine_count


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RiskFlow silver transformation")
    p.add_argument("--bronze-dir",     type=Path, required=True)
    p.add_argument("--silver-dir",     type=Path, required=True)
    p.add_argument("--quarantine-dir", type=Path, required=True)
    # Exactly one of --load-date / --load-dates must be provided. We
    # don't use a mutually-exclusive group with required=True because
    # argparse's error messages there are worse than what we can write
    # by hand; we validate in main() instead.
    p.add_argument(
        "--load-date", type=str, default=None,
        help="Single ISO date YYYY-MM-DD. Backward-compatible — used "
             "by the Phase 2 Airflow DAG and the parity test.",
    )
    p.add_argument(
        "--load-dates", type=str, default=None,
        help="Comma-separated ISO dates, e.g. 2026-04-01,2026-04-02,... "
             "Used by the Phase 5 perf harness to process N partitions "
             "in one Spark session.",
    )
    p.add_argument("--load-ts",        type=str,  required=True)
    p.add_argument("--run-id",         type=str,  required=True)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # Validate the one-of constraint by hand for a clean error message.
    if (args.load_date is None) == (args.load_dates is None):
        log.error(
            "Exactly one of --load-date or --load-dates must be specified."
        )
        return 2

    if args.load_dates is not None:
        load_dates = [d.strip() for d in args.load_dates.split(",") if d.strip()]
        if not load_dates:
            log.error("--load-dates was empty after parsing")
            return 2
        app_suffix = f"multi_{len(load_dates)}dates"
    else:
        load_dates = [args.load_date]
        app_suffix = args.load_date

    spark = (
        SparkSession.builder
        .appName(f"riskflow_silver_transform__{app_suffix}")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        if len(load_dates) == 1:
            # Backward-compatible single-date path. Same call shape the
            # Airflow DAG and parity test have always used.
            silver_count, quarantine_count = transform(
                spark,
                bronze_dir=args.bronze_dir,
                silver_dir=args.silver_dir,
                quarantine_dir=args.quarantine_dir,
                load_date=load_dates[0],
                load_ts=args.load_ts,
                run_id=args.run_id,
            )
        else:
            silver_count, quarantine_count = transform_multi(
                spark,
                bronze_dir=args.bronze_dir,
                silver_dir=args.silver_dir,
                quarantine_dir=args.quarantine_dir,
                load_dates=load_dates,
                load_ts=args.load_ts,
                run_id=args.run_id,
            )
    except Exception:
        log.exception("Silver transformation failed unexpectedly")
        return 1
    finally:
        spark.stop()

    print(f"SILVER_ROW_COUNT={silver_count}")
    print(f"QUARANTINE_ROW_COUNT={quarantine_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
