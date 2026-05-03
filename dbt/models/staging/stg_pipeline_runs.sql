-- Staging view over the operational pipeline_runs table.
-- Used only by gold_pipeline_quality_summary.

{{ config(materialized='view') }}

SELECT
    run_id,
    dag_id,
    partition_date,
    status,
    started_at,
    ended_at,
    duration_seconds,
    bronze_row_count,
    silver_row_count,
    failed_row_count,
    notes
FROM {{ source('raw_public', 'pipeline_runs') }}
