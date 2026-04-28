"""RiskFlow — bronze-layer lineage transformations.

This module contains the pure functions used by the bronze ingestion job.
Keeping them separate from the Spark glue is what makes ≥80% Pytest
coverage on the `transformations/` module achievable: these functions
take a DataFrame in and return a DataFrame out, with no I/O and no
configuration.

Convention: lineage columns added by the platform are prefixed with `_`
so they are visually distinct from source columns when querying.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------
# PaySim source schema
#
# We declare the schema explicitly rather than letting Spark infer it.
# Schema inference is convenient but silently masks upstream changes —
# if PaySim's CSV format ever shifts, we want bronze to fail loudly,
# not adopt the new shape and break silver tomorrow.
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
    T.StructField("isFlaggedFraud",  T.IntegerType(), nullable=False),
])

# Lineage columns this module adds. Exposed for downstream code (and
# tests) to know which columns are platform-added vs. source-derived.
LINEAGE_COLUMNS: tuple[str, ...] = ("_load_ts", "_source_file", "_run_id")


def add_lineage_columns(
    df: DataFrame,
    *,
    load_ts: str,
    source_file: str,
    run_id: str,
) -> DataFrame:
    """Append the three lineage columns to a bronze-bound DataFrame.

    Args:
        df: A DataFrame whose schema matches PAYSIM_SCHEMA.
        load_ts: ISO-8601 UTC timestamp string when the DAG run started,
            e.g. "2026-05-03T08:00:00Z". Stored as a TIMESTAMP, not STRING,
            so downstream queries can do range filters cheaply.
        source_file: Bare filename of the source CSV, e.g. "day_07.csv".
            Bare filename only — full paths leak environment details.
        run_id: The Airflow run identifier. Joins to pipeline_runs.run_id
            for end-to-end lineage from a bronze row back to a DAG run.

    Returns:
        DataFrame with three additional columns: _load_ts, _source_file, _run_id.

    Raises:
        ValueError: if any of load_ts, source_file, or run_id is empty.
    """
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
    """Raise if `df` does not exactly match PAYSIM_SCHEMA.

    Called by the Spark job immediately after reading the source CSV.
    A mismatch here is a loud, useful failure: it means the source
    format has shifted and bronze must NOT silently adopt the change.

    Tolerance policy: this is strict by design. Both missing AND extra
    columns raise. If PaySim ever adds a column we want, this function
    is the single place to update.
    """
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
