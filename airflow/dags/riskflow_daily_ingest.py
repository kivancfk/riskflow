"""RiskFlow — daily bronze ingestion DAG (Phase 1).

Triggered manually via:
    make run-day DAY=07
which dispatches:
    airflow dags trigger riskflow_daily_ingest --conf '{"day": "07"}'

The `day` conf value is required and selects which CSV partition
(data/partitioned/day_XX.csv) gets ingested.

Schedule semantics: declared @daily but stays paused during dev.
Phase 1 design note explains why (TL;DR: scheduler-ready, but data
is fixed and reviewers shouldn't wait 30 days). See docs/decisions.md.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ---------------------------------------------------------------
# Paths inside the Airflow container (set by docker-compose mounts)
# ---------------------------------------------------------------
SOURCE_DIR = "/opt/airflow/data/partitioned"
BRONZE_DIR = "/opt/airflow/data/bronze"
SPARK_JOB  = "/opt/airflow/spark_jobs/bronze_ingest.py"

# Postgres connection string — dedicated user, not Airflow's metadata DB
RISKFLOW_PG_DSN = "host=postgres port=5432 dbname=riskflow user=riskflow password=riskflow"

DEFAULT_ARGS = {
    "owner": "riskflow",
    "depends_on_past": False,
    "retries": 0,                      # validation failures must NOT retry
    "retry_delay": timedelta(minutes=2),
}


# ---------------------------------------------------------------
# Task 3 callable — record_pipeline_run
# ---------------------------------------------------------------
def _record_pipeline_run(**context) -> None:
    """Always-runs task that writes one row to pipeline_runs.

    Uses trigger_rule="all_done", so it runs whether ingest_to_bronze
    succeeded or failed. We want one row per attempt — that's how
    'pipeline success rate' becomes computable later.
    """
    ti = context["ti"]
    dag_run = context["dag_run"]
    day_param = dag_run.conf.get("day", "??")
    partition_date = context["ds"]      # logical execution date, ISO YYYY-MM-DD

    # Did ingest_to_bronze succeed?
    ingest_state = ti.xcom_pull(task_ids="ingest_to_bronze", key="return_value")
    upstream_states = [
        ti.get_dagrun().get_task_instance("ingest_to_bronze").current_state(),
    ]
    status = "success" if upstream_states[0] == "success" else "failed"

    # Pull the row count emitted by the Spark job's stdout (if it ran)
    bronze_row_count: int | None = None
    spark_logs: str | None = ti.xcom_pull(task_ids="ingest_to_bronze", key="return_value")
    # SparkSubmitOperator doesn't natively expose row count via XCom.
    # We instead probe the Parquet partition we just wrote.
    if status == "success":
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("count_check").getOrCreate()
            bronze_row_count = (
                spark.read.parquet(BRONZE_DIR)
                .filter(f"load_date = '{partition_date}'")
                .count()
            )
            spark.stop()
        except Exception:
            log.warning("Could not count bronze rows; leaving NULL.")

    # Compute duration from the upstream task's timestamps
    upstream_ti = ti.get_dagrun().get_task_instance("ingest_to_bronze")
    started_at = upstream_ti.start_date or datetime.now(timezone.utc)
    ended_at = upstream_ti.end_date or datetime.now(timezone.utc)
    duration_seconds = int((ended_at - started_at).total_seconds())

    run_uuid = str(uuid.uuid4())
    log.info(
        "Recording pipeline_run: id=%s status=%s rows=%s duration=%ss",
        run_uuid, status, bronze_row_count, duration_seconds,
    )

    with psycopg2.connect(RISKFLOW_PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, dag_id, partition_date, status,
                    started_at, ended_at, duration_seconds,
                    bronze_row_count, silver_row_count, failed_row_count,
                    notes
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, NULL, NULL,
                    %s
                )
                """,
                (
                    run_uuid,
                    "riskflow_daily_ingest",
                    partition_date,
                    status,
                    started_at,
                    ended_at,
                    duration_seconds,
                    bronze_row_count,
                    f"day_{day_param}.csv",
                ),
            )


# ---------------------------------------------------------------
# DAG
# ---------------------------------------------------------------
with DAG(
    dag_id="riskflow_daily_ingest",
    description="Phase 1: ingest one PaySim daily partition (CSV → bronze Parquet).",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 1),
    schedule="@daily",                 # production intent; paused in dev
    catchup=False,                     # never auto-backfill
    max_active_runs=1,                 # one run at a time
    tags=["riskflow", "bronze"],
    params={
        "day": Param(
            "01",
            type="string",
            pattern=r"^(0[1-9]|[12][0-9]|30)$",
            description="Two-digit day number (01–30) of the partition to ingest.",
        ),
    },
) as dag:

    # ---- Task 1: sense the source CSV ----
    check_source_file_exists = FileSensor(
        task_id="check_source_file_exists",
        filepath="{{ params.day | string }}",  # overridden below
        fs_conn_id="fs_default",
        # Compose the actual path through templating so the sensor
        # picks up the day param at runtime
        poke_interval=10,
        timeout=120,
        mode="poke",
    )
    # FileSensor.filepath is set via Jinja so {{ params.day }} resolves at runtime
    check_source_file_exists.filepath = (
        f"{SOURCE_DIR}/day_{{{{ params.day }}}}.csv"
    )

    # ---- Task 2: submit Spark ingestion job ----
    ingest_to_bronze = SparkSubmitOperator(
        task_id="ingest_to_bronze",
        application=SPARK_JOB,
        conn_id="spark_default",
        name="riskflow_bronze_ingest",
        application_args=[
            "--input-csv",  f"{SOURCE_DIR}/day_{{{{ params.day }}}}.csv",
            "--output-dir", BRONZE_DIR,
            "--load-date",  "{{ ds }}",
            "--load-ts",    "{{ ts }}",
            "--run-id",     "{{ run_id }}",
        ],
        # Make /opt/transformations importable inside the Spark job
        env_vars={"PYTHONPATH": "/opt/transformations"},
        verbose=False,
    )

    # ---- Task 3: record pipeline_run row, even on failure ----
    record_pipeline_run = PythonOperator(
        task_id="record_pipeline_run",
        python_callable=_record_pipeline_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    check_source_file_exists >> ingest_to_bronze >> record_pipeline_run
