-- gold_daily_fraud_summary
--
-- Audience: risk operations, leadership.
-- Question: "How much fraud volume did we have on each day, broken down by transaction type?"
--
-- Grain: one row per (load_date, type). 30 days × 5 types ≤ 150 rows.
--
-- CTE pattern is required because Postgres doesn't allow referencing
-- aliased columns from the same SELECT level.

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        load_date,
        type,
        COUNT(*)                                                 AS total_transactions,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)                AS fraud_count,
        SUM(amount)                                              AS total_amount,
        SUM(CASE WHEN is_fraud THEN amount ELSE 0 END)           AS fraud_amount
    FROM {{ ref('stg_silver_transactions') }}
    GROUP BY load_date, type
)

SELECT
    load_date::text || '|' || type AS daily_fraud_key,

    load_date,
    type,
    total_transactions,
    fraud_count,
    total_amount,
    fraud_amount,

    fraud_count::numeric / NULLIF(total_transactions, 0) AS fraud_transaction_rate,
    fraud_amount         / NULLIF(total_amount, 0)       AS fraud_amount_rate
FROM base
