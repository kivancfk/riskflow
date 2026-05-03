-- gold_customer_risk_features
--
-- Audience: ML scoring, fraud-model training.
-- Question: "What does this customer's transaction history look like as features?"
--
-- Grain: one row per origin customer (name_orig). Will produce ~6M rows
-- on the full PaySim dataset.

{{ config(materialized='table') }}

SELECT
    name_orig                                              AS customer_id,
    name_orig                                              AS customer_risk_features_key,

    COUNT(*)                                               AS total_transactions_30d,
    SUM(amount)                                            AS total_amount_30d,
    AVG(amount)                                            AS avg_amount_30d,
    MAX(amount)                                            AS max_amount_30d,
    MIN(amount)                                            AS min_amount_30d,

    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)              AS fraud_count_30d,
    BOOL_OR(is_fraud)                                      AS is_fraud_ever_flag,
    BOOL_OR(is_flagged_fraud)                              AS is_flagged_ever_flag,

    COUNT(DISTINCT name_dest)                              AS unique_destinations_30d,
    COUNT(DISTINCT type)                                   AS unique_types_30d,

    MIN(load_date)                                         AS first_txn_date,
    MAX(load_date)                                         AS last_txn_date,
    (MAX(load_date) - MIN(load_date))                      AS customer_age_days
FROM {{ ref('stg_silver_transactions') }}
GROUP BY name_orig
