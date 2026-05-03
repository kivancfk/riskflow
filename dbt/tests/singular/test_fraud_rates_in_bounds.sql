-- Singular test: fraud rates must lie in [0, 1].
--
-- Returns rows that VIOLATE the test. Empty result = pass.
--
-- We do NOT test specific PaySim-realistic bounds per type.
-- Some (day, type) pairs may have zero fraud (e.g. CASH_IN often has none),
-- and that's correct behavior — not a bug. Bounds [0, 1] catch real corruption
-- without flagging legitimate empty cells.

SELECT
    daily_fraud_key,
    load_date,
    type,
    fraud_transaction_rate,
    fraud_amount_rate
FROM {{ ref('gold_daily_fraud_summary') }}
WHERE
       fraud_transaction_rate < 0
    OR fraud_transaction_rate > 1
    OR fraud_amount_rate      < 0
    OR fraud_amount_rate      > 1
