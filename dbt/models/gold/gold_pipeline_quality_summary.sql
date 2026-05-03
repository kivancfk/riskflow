-- gold_pipeline_quality_summary
--
-- Audience: data platform team, on-call engineering.
-- Question: "Is the pipeline healthy? What's the dropout rate?"
--
-- Grain: one row per (partition_date, dag_id).

{{ config(materialized='table') }}

WITH latest_per_partition AS (
    SELECT DISTINCT ON (partition_date, dag_id)
        partition_date,
        dag_id,
        status,
        duration_seconds,
        bronze_row_count,
        silver_row_count,
        failed_row_count
    FROM {{ ref('stg_pipeline_runs') }}
    ORDER BY partition_date, dag_id, started_at DESC
),

run_aggregates AS (
    SELECT
        partition_date,
        dag_id,
        COUNT(*)                                                  AS total_attempts,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)       AS success_count,
        SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END)       AS failed_count,
        AVG(duration_seconds)                                     AS avg_duration_seconds,
        MAX(duration_seconds)                                     AS max_duration_seconds
    FROM {{ ref('stg_pipeline_runs') }}
    GROUP BY partition_date, dag_id
)

SELECT
    l.partition_date::text || '|' || l.dag_id            AS pipeline_quality_key,

    l.partition_date,
    l.dag_id,

    a.total_attempts,
    a.success_count,
    a.failed_count,
    a.avg_duration_seconds,
    a.max_duration_seconds,

    l.bronze_row_count,
    l.silver_row_count,
    l.failed_row_count,

    -- bronze_row_count is currently NULL for Phase 2 runs (deferred fix);
    -- this metric will populate after Phase 6 polish.
    l.silver_row_count::numeric / NULLIF(l.bronze_row_count, 0)
        AS bronze_to_silver_retention_rate
FROM latest_per_partition l
JOIN run_aggregates       a USING (partition_date, dag_id)
