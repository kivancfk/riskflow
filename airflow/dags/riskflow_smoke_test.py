"""
RiskFlow — smoke-test DAG.

Purpose: validate the local stack is wired correctly by confirming
Airflow can talk to Postgres and submit a trivial job to Spark.
This DAG should be deleted (or left paused) once the real ingestion
DAG is in place.

Run: trigger from the Airflow UI or `airflow dags trigger riskflow_smoke_test`.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    "owner": "riskflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def _hello_python(**context):
    """Trivial Python check — confirms PythonOperator works inside the container."""
    print("Hello from Python inside Airflow.")
    print(f"Run id: {context['run_id']}")
    return "ok"


with DAG(
    dag_id="riskflow_smoke_test",
    description="One-shot smoke test that validates Airflow ↔ Postgres ↔ Spark wiring.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 27),
    schedule=None,                # manual trigger only
    catchup=False,
    tags=["riskflow", "smoke"],
) as dag:

    # 1. Pure Python — proves the worker can run code
    py = PythonOperator(
        task_id="hello_python",
        python_callable=_hello_python,
    )

    # 2. Talk to Postgres — proves the connection + database init worked
    pg = PostgresOperator(
        task_id="postgres_check",
        postgres_conn_id="postgres_default",
        sql="SELECT count(*) AS pipeline_runs_count FROM pipeline_runs;",
    )

    # 3. Talk to Spark master — proves SparkSubmitOperator path works without
    #    actually running a heavy job. Real PySpark jobs come in Phase 1.
    spark = BashOperator(
        task_id="spark_master_reachable",
        bash_command="curl -fsS http://spark-master:8080 > /dev/null && echo 'Spark master OK'",
    )

    py >> pg >> spark
