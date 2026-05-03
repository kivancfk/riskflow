-- gold_transaction_velocity_features
--
-- Audience: real-time fraud detection.
-- Question: "How fast is this customer transacting and is the velocity changing?"
--
-- Grain: one row per (customer, day, hour).

{{ config(materialized='table') }}

WITH hourly AS (
    SELECT
        name_orig,
        load_date,
        event_hour,
        COUNT(*)                                  AS txn_count_in_hour,
        SUM(amount)                               AS total_amount_in_hour,
        COUNT(DISTINCT name_dest)                 AS unique_dest_count_in_hour,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count_in_hour
    FROM {{ ref('stg_silver_transactions') }}
    GROUP BY name_orig, load_date, event_hour
)

SELECT
    name_orig || '|' || load_date::text || '|' || event_hour::text AS velocity_key,

    name_orig                       AS customer_id,
    load_date,
    event_hour,

    txn_count_in_hour,
    total_amount_in_hour,
    unique_dest_count_in_hour,
    fraud_count_in_hour,

    LAG(txn_count_in_hour) OVER (
        PARTITION BY name_orig
        ORDER BY load_date, event_hour
    ) AS prev_hour_txn_count,

    txn_count_in_hour - LAG(txn_count_in_hour) OVER (
        PARTITION BY name_orig
        ORDER BY load_date, event_hour
    ) AS velocity_change_vs_previous_hour
FROM hourly
