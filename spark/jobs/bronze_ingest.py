"""RiskFlow — bronze ingestion (CSV → Parquet).

Spark job submitted by the riskflow_daily_ingest DAG.

Read one daily PaySim partition from `--input-csv`, validate its
schema matches PAYSIM_SCHEMA, append the three lineage columns, and
write Parquet to `--output-dir` partitioned by `load_date`.

Idempotency: writes use mode="overwrite" with partitionBy("load_date"),
so re-running the same partition_date replaces only that partition,
not the whole bronze tree. ADR-004.

Usage (called by SparkSubmitOperator):
    spark-submit bronze_ingest.py \\
        --input-csv     /opt/data/partitioned/day_07.csv \\
        --output-dir    /opt/data/bronze \\
        --load-date     2026-05-03 \\
        --load-ts       2026-05-03T08:00:00Z \\
        --run-id        manual__2026-05-03T08:00:00.123456+00:00
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# When run by SparkSubmitOperator, /opt/transformations is on the
# Python path because docker-compose mounts it into the Spark image.
from transformations import (
    PAYSIM_SCHEMA,
    add_lineage_columns,
    validate_paysim_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bronze_ingest")


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

    # Schema check (loud failure if upstream drifts)
    validate_paysim_schema(df)

    # Add lineage and the partition-key column
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

    # Write Parquet, partitioned by load_date for idempotent re-runs
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
    p.add_argument("--load-date",   type=str,  required=True,
                   help="ISO date for the bronze partition, e.g. 2026-05-03")
    p.add_argument("--load-ts",     type=str,  required=True,
                   help="ISO-8601 UTC timestamp the DAG run started at")
    p.add_argument("--run-id",      type=str,  required=True,
                   help="Airflow run identifier — joins to pipeline_runs.run_id")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.input_csv.exists():
        log.error("Input CSV not found: %s", args.input_csv)
        return 1

    spark = (
        SparkSession.builder
        .appName(f"riskflow_bronze_ingest__{args.load_date}")
        # Adaptive Query Execution on by default — Phase 5 will demonstrate
        # turning this off and back on to show its impact.
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

    # Print the row count to stdout in a parseable form so the Airflow
    # task can capture it via XCom (the SparkSubmitOperator surfaces
    # stdout in its logs and downstream tasks can read it back).
    print(f"BRONZE_ROW_COUNT={row_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
