"""RiskFlow — daily ingestion DAG (Phase 3 v2).

Phase 3 additions (vs Phase 2's 6 tasks):
  - load_silver_to_postgres: PySpark task that writes the day's silver
    partition into staging.silver_transactions via JDBC. Idempotent —
    DELETEs the day's existing rows before writing.
  - run_dbt_gold: BashOperator that runs `dbt build --select +gold`,
    which materializes both staging views AND gold tables, plus runs
    all schema and singular tests in one command.

DAG flow (8 tasks):
  check_source_file_exists
       ↓
  ingest_to_bronze
       ↓
  validate_bronze            (GE checkpoint)
       ↓
  transform_to_silver
       ↓
  validate_silver            (GE checkpoint)
       ↓
  load_silver_to_postgres    ← Phase 3
       ↓
  run_dbt_gold               ← Phase 3
       ↓
  record_pipeline_run        (always-runs)
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

SOURCE_DIR     = "/opt/airflow/data/partitioned"
BRONZE_DIR     = "/opt/airflow/data/bronze"
SILVER_DIR     = "/opt/airflow/data/silver"
QUARANTINE_DIR = "/opt/airflow/data/silver_quarantine"
BRONZE_JOB     = "/opt/airflow/spark_jobs/bronze_ingest.py"
SILVER_JOB     = "/opt/airflow/spark_jobs/silver_transform.py"
SILVER_TO_PG_JOB = "/opt/airflow/spark_jobs/silver_to_postgres.py"
GE_ROOT        = "/opt/airflow/great_expectations"
DBT_PROJECT    = "/opt/airflow/dbt"

JDBC_URL       = "jdbc:postgresql://postgres:5432/riskflow"
JDBC_USER      = "riskflow"
JDBC_PASSWORD  = "riskflow"
TARGET_TABLE   = "staging.silver_transactions"

RISKFLOW_PG_DSN = (
    "host=postgres port=5432 dbname=riskflow user=riskflow password=riskflow"
)

DEFAULT_ARGS = {
    "owner": "riskflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def _zero_padded_day(dag_run) -> str:
    raw = (dag_run.conf or {}).get("day", "1")
    return f"{int(raw):02d}"


def _validate_bronze(**context) -> None:
    import great_expectations as gx
    from airflow.exceptions import AirflowException
    from pyspark.sql import SparkSession

    dag_run = context["dag_run"]
    day = _zero_padded_day(dag_run)
    load_date = f"2026-04-{day}"
    partition_path = f"{BRONZE_DIR}/load_date={load_date}"

    log.info("GE bronze checkpoint for load_date=%s", load_date)

    spark = (
        SparkSession.builder
        .appName("ge_bronze_read")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        pandas_df = spark.read.parquet(partition_path).toPandas()
    finally:
        spark.stop()

    log.info("Validating %s bronze rows", f"{len(pandas_df):,}")

    context_gx = gx.get_context(context_root_dir=GE_ROOT)
    result = context_gx.run_checkpoint(
        checkpoint_name="bronze_checkpoint",
        batch_request={
            "datasource_name": "bronze_pandas",
            "data_asset_name": "bronze_partition",
            "runtime_parameters": {"batch_data": pandas_df},
            "batch_identifiers": {"load_date": load_date},
        },
    )

    if not result["success"]:
        failed = [k for k, v in result["run_results"].items()
                  if not v["validation_result"]["success"]]
        raise AirflowException(
            f"Bronze checkpoint failed for load_date={load_date}: {failed}"
        )

    log.info("Bronze checkpoint passed for load_date=%s", load_date)


def _validate_silver(**context) -> None:
    import great_expectations as gx
    from airflow.exceptions import AirflowException
    from pyspark.sql import SparkSession

    dag_run = context["dag_run"]
    day = _zero_padded_day(dag_run)
    load_date = f"2026-04-{day}"
    silver_path = f"{SILVER_DIR}/load_date={load_date}"

    log.info("GE silver checkpoint for load_date=%s", load_date)

    spark = (
        SparkSession.builder
        .appName("ge_silver_read")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        df = spark.read.parquet(silver_path)
        row_count = df.count()
        pandas_df = df.toPandas()
    finally:
        spark.stop()

    log.info("Validating %s silver rows", f"{row_count:,}")

    context_gx = gx.get_context(context_root_dir=GE_ROOT)
    result = context_gx.run_checkpoint(
        checkpoint_name="silver_checkpoint",
        batch_request={
            "datasource_name": "silver_pandas",
            "data_asset_name": "silver_partition",
            "runtime_parameters": {"batch_data": pandas_df},
            "batch_identifiers": {"load_date": load_date},
        },
    )

    if not result["success"]:
        failed = [k for k, v in result["run_results"].items()
                  if not v["validation_result"]["success"]]
        raise AirflowException(
            f"Silver checkpoint failed for load_date={load_date}: {failed}"
        )

    context["ti"].xcom_push(key="silver_row_count", value=row_count)
    log.info("Silver checkpoint passed for load_date=%s", load_date)


def _record_pipeline_run(**context) -> None:
    """Always-runs task. Records dbt success/failure as the upstream-of-record."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    day = _zero_padded_day(dag_run)
    partition_date = context["ds"]

    upstream_ti = ti.get_dagrun().get_task_instance("run_dbt_gold")
    upstream_state = upstream_ti.current_state() if upstream_ti else None
    status = "success" if upstream_state == "success" else "failed"

    silver_row_count: int | None = ti.xcom_pull(
        task_ids="validate_silver", key="silver_row_count"
    )

    started_at = (
        upstream_ti.start_date if upstream_ti and upstream_ti.start_date
        else datetime.now(timezone.utc)
    )
    ended_at = (
        upstream_ti.end_date if upstream_ti and upstream_ti.end_date
        else datetime.now(timezone.utc)
    )
    duration_seconds = int((ended_at - started_at).total_seconds())

    run_uuid = str(uuid.uuid4())
    log.info(
        "Recording pipeline_run: id=%s status=%s silver_rows=%s",
        run_uuid, status, silver_row_count,
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
                    NULL, %s, NULL,
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
                    silver_row_count,
                    f"day_{day}.csv",
                ),
            )


with DAG(
    dag_id="riskflow_daily_ingest",
    description="Phase 3: bronze + silver + dbt gold marts.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["riskflow", "bronze", "silver", "gold", "dbt"],
    params={"day": "01"},
) as dag:

    check_source_file_exists = FileSensor(
        task_id="check_source_file_exists",
        filepath=f"{SOURCE_DIR}/day_{{{{ params.day }}}}.csv",
        poke_interval=10,
        timeout=120,
        mode="poke",
    )

    ingest_to_bronze = SparkSubmitOperator(
        task_id="ingest_to_bronze",
        application=BRONZE_JOB,
        conn_id="spark_default",
        name="riskflow_bronze_ingest",
        application_args=[
            "--input-csv",  f"{SOURCE_DIR}/day_{{{{ params.day }}}}.csv",
            "--output-dir", BRONZE_DIR,
            "--load-date",  "2026-04-{{ '%02d' % (params.day | int) }}",
            "--load-ts",    "{{ ts }}",
            "--run-id",     "{{ run_id }}",
        ],
        verbose=False,
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=_validate_bronze,
    )

    transform_to_silver = SparkSubmitOperator(
        task_id="transform_to_silver",
        application=SILVER_JOB,
        conn_id="spark_default",
        name="riskflow_silver_transform",
        application_args=[
            "--bronze-dir",     BRONZE_DIR,
            "--silver-dir",     SILVER_DIR,
            "--quarantine-dir", QUARANTINE_DIR,
            "--load-date",      "2026-04-{{ '%02d' % (params.day | int) }}",
            "--load-ts",        "{{ ts }}",
            "--run-id",         "{{ run_id }}",
        ],
        verbose=False,
    )

    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=_validate_silver,
    )

    load_silver_to_postgres = SparkSubmitOperator(
        task_id="load_silver_to_postgres",
        application=SILVER_TO_PG_JOB,
        conn_id="spark_default",
        name="riskflow_silver_to_postgres",
        packages="org.postgresql:postgresql:42.7.4",
        application_args=[
            "--silver-dir",     SILVER_DIR,
            "--load-date",      "2026-04-{{ '%02d' % (params.day | int) }}",
            "--jdbc-url",       JDBC_URL,
            "--jdbc-user",      JDBC_USER,
            "--jdbc-password",  JDBC_PASSWORD,
            "--target-table",   TARGET_TABLE,
        ],
        verbose=False,
    )

    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command=(
            f"cd {DBT_PROJECT} && "
            f"DBT_PROFILES_DIR={DBT_PROJECT} "
            f"dbt build --select +gold --no-version-check"
        ),
    )

    record_pipeline_run = PythonOperator(
        task_id="record_pipeline_run",
        python_callable=_record_pipeline_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        check_source_file_exists
        >> ingest_to_bronze
        >> validate_bronze
        >> transform_to_silver
        >> validate_silver
        >> load_silver_to_postgres
        >> run_dbt_gold
        >> record_pipeline_run
    )
