"""RiskFlow — silver Parquet → Postgres staging (Phase 3 v2).

Reads one silver load_date partition and writes it to
staging.silver_transactions via JDBC.

Idempotency strategy:
  Before writing, DELETE rows where load_date = <current load_date>.
  Then append the current partition. Rerunning DAY=07 produces the
  same final row count, never duplicates.

Why DELETE+append (not Spark's overwrite):
  Spark's JDBC overwrite truncates the entire target table, not just
  the current load_date. That would wipe other days' data. We need
  partition-level idempotency, which Postgres does via DELETE WHERE.

Usage (called by SparkSubmitOperator):
    spark-submit silver_to_postgres.py \\
        --silver-dir   /opt/airflow/data/silver \\
        --load-date    2026-04-07 \\
        --jdbc-url     jdbc:postgresql://postgres:5432/riskflow \\
        --jdbc-user    riskflow \\
        --jdbc-password riskflow \\
        --target-table staging.silver_transactions
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg2
from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("silver_to_postgres")


# ---------------------------------------------------------------
# Pure helper: parse JDBC URL into psycopg2 DSN parts.
# Tested in isolation; no Spark dependency.
# ---------------------------------------------------------------
def jdbc_url_to_psycopg2_dsn(jdbc_url: str, user: str, password: str) -> str:
    """Convert 'jdbc:postgresql://host:port/db' to a psycopg2 DSN string.

    Pure function — no I/O, no Spark. Easy to unit-test.
    """
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError(f"Expected JDBC URL prefix 'jdbc:postgresql://', got: {jdbc_url}")

    rest = jdbc_url[len("jdbc:postgresql://"):]
    if "/" not in rest:
        raise ValueError(f"JDBC URL missing database: {jdbc_url}")

    host_port, dbname = rest.split("/", 1)
    if ":" in host_port:
        host, port = host_port.split(":", 1)
    else:
        host, port = host_port, "5432"

    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def delete_partition(
    psycopg2_dsn: str,
    target_table: str,
    load_date: str,
) -> int:
    """Delete rows for the given load_date. Returns rowcount deleted."""
    with psycopg2.connect(psycopg2_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {target_table} WHERE load_date = %s",
                (load_date,),
            )
            return cur.rowcount


def write_partition(
    spark: SparkSession,
    *,
    silver_dir: str,
    load_date: str,
    jdbc_url: str,
    jdbc_user: str,
    jdbc_password: str,
    target_table: str,
) -> int:
    """Read one silver partition and append it to Postgres. Returns row count."""
    partition_path = f"{silver_dir}/load_date={load_date}"
    log.info("Reading silver partition: %s", partition_path)

    df: DataFrame = spark.read.parquet(partition_path)

    # Re-add load_date as a regular column (it's the partition col, not in the file body)
    from pyspark.sql import functions as F
    df = df.withColumn("load_date", F.to_date(F.lit(load_date)))

    row_count = df.count()
    log.info("Rows to write: %s", f"{row_count:,}")

    log.info("Writing to Postgres: %s", target_table)
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", target_table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "10000")
        .option("rewriteBatchedStatements", "true")
        .mode("append")
        .save()
    )

    return row_count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Silver Parquet → Postgres staging")
    p.add_argument("--silver-dir",     type=str, required=True)
    p.add_argument("--load-date",      type=str, required=True)
    p.add_argument("--jdbc-url",       type=str, required=True)
    p.add_argument("--jdbc-user",      type=str, required=True)
    p.add_argument("--jdbc-password",  type=str, required=True)
    p.add_argument("--target-table",   type=str, required=True)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    dsn = jdbc_url_to_psycopg2_dsn(
        args.jdbc_url, args.jdbc_user, args.jdbc_password,
    )

    log.info("Step 1: deleting existing rows for load_date=%s", args.load_date)
    deleted = delete_partition(dsn, args.target_table, args.load_date)
    log.info("Deleted %s existing rows", f"{deleted:,}")

    log.info("Step 2: writing silver partition to Postgres")
    spark = (
        SparkSession.builder
        .appName(f"riskflow_silver_to_postgres__{args.load_date}")
        .getOrCreate()
    )

    try:
        rows = write_partition(
            spark,
            silver_dir=args.silver_dir,
            load_date=args.load_date,
            jdbc_url=args.jdbc_url,
            jdbc_user=args.jdbc_user,
            jdbc_password=args.jdbc_password,
            target_table=args.target_table,
        )
    except Exception:
        log.exception("Silver→Postgres write failed unexpectedly")
        return 1
    finally:
        spark.stop()

    log.info("Wrote %s rows to %s for load_date=%s",
             f"{rows:,}", args.target_table, args.load_date)
    print(f"POSTGRES_ROW_COUNT={rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
