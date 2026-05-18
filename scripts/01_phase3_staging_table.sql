-- RiskFlow — Phase 3 staging table DDL + schema bootstrap
--
-- Schema layout:
--   staging      → raw Spark JDBC table (silver_transactions)
--   dbt_staging  → dbt-managed staging views
--   gold         → dbt-managed gold tables
--   public       → operational pipeline_runs (Phase 1, already exists)
--
-- Apply once after Postgres comes up:
--   make pg-init-staging

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dbt_staging;
CREATE SCHEMA IF NOT EXISTS gold;

-- The staging table mirrors silver Parquet schema 1:1.
-- Spark JDBC does NOT create this automatically — must exist before write.
CREATE TABLE IF NOT EXISTS staging.silver_transactions (
    step                INTEGER,
    type                VARCHAR(16),
    amount              NUMERIC(18, 2),
    name_orig           VARCHAR(32),
    old_balance_orig    NUMERIC(18, 2),
    new_balance_orig    NUMERIC(18, 2),
    name_dest           VARCHAR(32),
    old_balance_dest    NUMERIC(18, 2),
    new_balance_dest    NUMERIC(18, 2),
    _load_ts            TIMESTAMP,
    _source_file        VARCHAR(64),
    _run_id             VARCHAR(128),
    is_fraud            BOOLEAN,
    is_flagged_fraud    BOOLEAN,
    event_hour          INTEGER,
    balance_delta_orig  NUMERIC(18, 2),
    balance_delta_dest  NUMERIC(18, 2),
    load_date           DATE NOT NULL
);

-- Indexes that gold marts will benefit from
CREATE INDEX IF NOT EXISTS idx_silver_load_date
    ON staging.silver_transactions(load_date);

CREATE INDEX IF NOT EXISTS idx_silver_name_orig
    ON staging.silver_transactions(name_orig);

CREATE INDEX IF NOT EXISTS idx_silver_type
    ON staging.silver_transactions(type);

-- Composite index for velocity feature aggregations
CREATE INDEX IF NOT EXISTS idx_silver_orig_date_hour
    ON staging.silver_transactions(name_orig, load_date, event_hour);

GRANT ALL ON SCHEMA staging      TO riskflow;
GRANT ALL ON SCHEMA dbt_staging  TO riskflow;
GRANT ALL ON SCHEMA gold         TO riskflow;
GRANT ALL ON ALL TABLES IN SCHEMA staging      TO riskflow;
GRANT ALL ON ALL TABLES IN SCHEMA dbt_staging  TO riskflow;
GRANT ALL ON ALL TABLES IN SCHEMA gold         TO riskflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging      GRANT ALL ON TABLES TO riskflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_staging  GRANT ALL ON TABLES TO riskflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold         GRANT ALL ON TABLES TO riskflow;
