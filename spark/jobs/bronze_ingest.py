"""RiskFlow — bronze ingestion (CSV → Parquet).

Spark job submitted by the riskflow_daily_ingest DAG.

Self-contained: PAYSIM_SCHEMA, add_lineage_columns(), and
validate_paysim_schema() are defined in this file rather than imported
from transformations/. PySpark's spark-submit doesn't reliably surface
PYTHONPATH from Airflow's env_vars to the driver's Python interpreter,
and bundling --py-files for a single small module is overkill for the
amount of code we have. Pytest still tests the same functions —
test_lineage.py imports them from THIS file as the source of truth.

Idempotency: writes use mode="overwrite" with partitionBy("load_date"),
so re-running the same partition_date replaces only that partition.

Usage (called by SparkSubmitOperator):
    spark-submit bronze_ingest.py \\
        --input-csv     /opt/airflow/data/partitioned/day_07.csv \\
        --output-dir    /opt/airflow/data/bronze \\
        --load-date     2026-04-28 \\
        --load-ts       2026-04-28T08:00:00Z \\
        --run-id        manual__2026-04-28T08:00:00.123456+00:00
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
log = logging.getLogger("bronze_ingest")


# ---------------------------------------------------------------
# PaySim source schema (declared explicitly — no inference)
# ---------------------------------------------------------------
PAYSIM_SCHEMA = T.StructType([
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
    T.StructField("isFlaggedFraud", T.IntegerType(), nullable=False),
])

LINEAGE_COLUMNS: tuple[str, ...] = ("_load_ts", "_source_file", "_run_id")


# ---------------------------------------------------------------
# Pure functions (Pytest-testable, no I/O, no SparkSession-binding)
# ---------------------------------------------------------------
def add_lineage_columns(
    df: DataFrame,
    *,
    load_ts: str,
    source_file: str,
    run_id: str,
) -> DataFrame:
    """Append the three lineage columns to a bronze-bound DataFrame."""
    if not load_ts:
        raise ValueError("load_ts must be a non-empty ISO-8601 timestamp")
    if not source_file:
        raise ValueError("source_file must be a non-empty filename")
    if not run_id:
        raise ValueError("run_id must be a non-empty Airflow run identifier")

    return (
        df
        .withColumn("_load_ts",     F.to_timestamp(F.lit(load_ts)))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_run_id",      F.lit(run_id))
    )


def validate_paysim_schema(df: DataFrame) -> None:
    """Raise if `df` does not exactly match PAYSIM_SCHEMA."""
    expected = {field.name for field in PAYSIM_SCHEMA.fields}
    actual = set(df.columns)

    missing = expected - actual
    extra = actual - expected

    problems: list[str] = []
    if missing:
        problems.append(f"missing columns: {sorted(missing)}")
    if extra:
        problems.append(f"unexpected columns: {sorted(extra)}")

    if problems:
        raise ValueError(
            "Source CSV schema does not match PAYSIM_SCHEMA — "
            + "; ".join(problems)
        )


# ---------------------------------------------------------------
# Driver
# ---------------------------------------------------------------
def ingest(
    spark: SparkSession,
    *,
    input_csv: Path,
    output_dir: Path,
    load_date: str,
    load_ts: str,
    run_id: str,
) -> int:
    """Read input CSV, write Parquet partition, return row count."""
    log.info("Reading %s with explicit PAYSIM_SCHEMA", input_csv)

    df: DataFrame = (
        spark.read
        .option("header", "true")
        .schema(PAYSIM_SCHEMA)
        .csv(str(input_csv))
    )

    validate_paysim_schema(df)

    enriched = (
        add_lineage_columns(
            df,
            load_ts=load_ts,
            source_file=input_csv.name,
            run_id=run_id,
        )
        .withColumn("load_date", F.to_date(F.lit(load_date)))
    )

    row_count = enriched.count()
    log.info("Read %s rows from %s", f"{row_count:,}", input_csv.name)

    log.info("Writing to %s/load_date=%s/", output_dir, load_date)
    (
        enriched.write
        .mode("overwrite")
        .partitionBy("load_date")
        .parquet(str(output_dir))
    )

    log.info("Bronze partition written successfully")
    return row_count


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--input-csv",   type=Path, required=True)
    p.add_argument("--output-dir",  type=Path, required=True)
    p.add_argument("--load-date",   type=str,  required=True)
    p.add_argument("--load-ts",     type=str,  required=True)
    p.add_argument("--run-id",      type=str,  required=True)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.input_csv.exists():
        log.error("Input CSV not found: %s", args.input_csv)
        return 1

    spark = (
        SparkSession.builder
        .appName(f"riskflow_bronze_ingest__{args.load_date}")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        row_count = ingest(
            spark,
            input_csv=args.input_csv,
            output_dir=args.output_dir,
            load_date=args.load_date,
            load_ts=args.load_ts,
            run_id=args.run_id,
        )
    except ValueError as e:
        log.error("Schema validation failed: %s", e)
        return 2
    except Exception:
        log.exception("Bronze ingestion failed unexpectedly")
        return 3
    finally:
        spark.stop()

    print(f"BRONZE_ROW_COUNT={row_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
