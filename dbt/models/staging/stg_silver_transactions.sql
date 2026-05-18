-- Staging view over the Postgres table that Spark JDBC populates.
-- Materialized as a view in dbt_staging.* schema (per dbt_project.yml).
-- Source columns are already snake_case in the Phase 5+ silver schema;
-- this view exists to rename a few lineage columns and to provide a
-- stable dbt-internal name for downstream gold consumption.

{{ config(materialized='view') }}

SELECT
    step,
    type,
    amount,
    name_orig,
    old_balance_orig,
    new_balance_orig,
    name_dest,
    old_balance_dest,
    new_balance_dest,
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
