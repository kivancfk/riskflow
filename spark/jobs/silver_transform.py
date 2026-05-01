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
DEDUP_KEY = ["nameOrig", "nameDest", "step", "amount"]
REQUIRED_FIELDS = ["step", "type", "amount", "nameOrig", "nameDest"]


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
    decimal_cols = [
        "amount", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest",
    ]
    result = df
    for col in decimal_cols:
        result = result.withColumn(col, F.col(col).cast(T.DecimalType(18, 2)))

    # Rename + cast in a single pass
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
    balance_delta_orig: newbalanceOrig - oldbalanceOrg.
    balance_delta_dest: newbalanceDest - oldbalanceDest.
    """
    return (
        df
        .withColumn(
            "event_hour",
            ((F.col("step") - 1) % 24).cast(T.IntegerType()),
        )
        .withColumn(
            "balance_delta_orig",
            (F.col("newbalanceOrig") - F.col("oldbalanceOrg"))
            .cast(T.DecimalType(18, 2)),
        )
        .withColumn(
            "balance_delta_dest",
            (F.col("newbalanceDest") - F.col("oldbalanceDest"))
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


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RiskFlow silver transformation")
    p.add_argument("--bronze-dir",     type=Path, required=True)
    p.add_argument("--silver-dir",     type=Path, required=True)
    p.add_argument("--quarantine-dir", type=Path, required=True)
    p.add_argument("--load-date",      type=str,  required=True)
    p.add_argument("--load-ts",        type=str,  required=True)
    p.add_argument("--run-id",         type=str,  required=True)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    spark = (
        SparkSession.builder
        .appName(f"riskflow_silver_transform__{args.load_date}")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        silver_count, quarantine_count = transform(
            spark,
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
    finally:
        spark.stop()

    print(f"SILVER_ROW_COUNT={silver_count}")
    print(f"QUARANTINE_ROW_COUNT={quarantine_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
