-- Staging view over the Postgres table that Spark JDBC populates.
-- Materialized as a view in dbt_staging.* schema (per dbt_project.yml).
-- Source data lives in staging.silver_transactions; this view normalizes
-- the camelCase columns into snake_case for downstream gold consumption.

{{ config(materialized='view') }}

SELECT
    step,
    type,
    amount,
    "nameOrig"           AS name_orig,
    "oldbalanceOrg"      AS old_balance_orig,
    "newbalanceOrig"     AS new_balance_orig,
    "nameDest"           AS name_dest,
    "oldbalanceDest"     AS old_balance_dest,
    "newbalanceDest"     AS new_balance_dest,
    is_fraud,
    is_flagged_fraud,
    event_hour,
    balance_delta_orig,
    balance_delta_dest,
    load_date,
    _load_ts             AS loaded_at,
    _source_file         AS source_file,
    _run_id              AS source_run_id
FROM {{ source('raw_staging', 'silver_transactions') }}
