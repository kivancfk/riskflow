-- RiskFlow — Postgres bootstrap
-- Runs once when the postgres container starts for the first time.
-- Creates:
--   1. The riskflow database (the warehouse for dbt + serving)
--   2. The riskflow user (separate from airflow user, principle of least privilege)
--   3. Monitoring tables in the riskflow database

-- Create the riskflow user
CREATE USER riskflow WITH PASSWORD 'riskflow';

-- Create the riskflow database, owned by riskflow
CREATE DATABASE riskflow WITH OWNER = riskflow;

-- Switch into the new database to create our monitoring tables
\connect riskflow

-- ---------------------------------------------------------------
-- pipeline_runs
--
-- One row per Airflow DAG run. Populated by the final task of
-- riskflow_daily_ingest. Used by gold_pipeline_quality_summary.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              UUID PRIMARY KEY,
    dag_id              TEXT NOT NULL,
    partition_date      DATE NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('success', 'failed', 'validation_failed')),
    started_at          TIMESTAMP NOT NULL,
    ended_at            TIMESTAMP,
    duration_seconds    INTEGER,
    bronze_row_count    BIGINT,
    silver_row_count    BIGINT,
    failed_row_count    BIGINT,
    notes               TEXT
);

CREATE INDEX idx_pipeline_runs_partition ON pipeline_runs (partition_date);
CREATE INDEX idx_pipeline_runs_status    ON pipeline_runs (status);

-- ---------------------------------------------------------------
-- failed_records
--
-- Rows that failed Great Expectations validation between
-- bronze and silver. Quarantined here for investigation rather
-- than silently dropped.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS failed_records (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID NOT NULL REFERENCES pipeline_runs(run_id),
    transaction_id      TEXT,
    rule_name           TEXT NOT NULL,
    reason              TEXT,
    raw_row             JSONB,
    failed_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_failed_records_run  ON failed_records (run_id);
CREATE INDEX idx_failed_records_rule ON failed_records (rule_name);

-- Grant the riskflow user full ownership of these tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO riskflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO riskflow;

-- ---------------------------------------------------------------
-- Schemas for dbt to use
-- ---------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION riskflow;
CREATE SCHEMA IF NOT EXISTS marts   AUTHORIZATION riskflow;
