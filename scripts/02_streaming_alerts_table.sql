-- ============================================================================
-- Phase 4 — streaming_alerts DDL
-- ============================================================================
-- Operational table populated by streaming/consumer.py when the velocity rule
-- flags a transaction. NOT an analytical mart — does not live in `gold`.
--
-- Idempotency: the (transaction_id, rule_name) UNIQUE constraint combined with
-- the consumer's `INSERT ... ON CONFLICT DO NOTHING` makes alert writes safe
-- to replay. This gives us effectively-once outcomes on top of Kafka's
-- at-least-once delivery, without requiring exactly-once semantics on the
-- broker.
--
-- Run once per environment:
--   docker compose exec -T postgres psql -U riskflow -d riskflow \
--     < scripts/02_streaming_alerts_table.sql
-- Idempotent — safe to re-run.
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.streaming_alerts (
    alert_id              BIGSERIAL PRIMARY KEY,

    -- Source-message identity
    transaction_id        VARCHAR(64)  NOT NULL,
    name_orig             VARCHAR(32)  NOT NULL,
    transaction_amount    NUMERIC(18, 2),
    transaction_type      VARCHAR(16),

    -- Rule outcome
    flagged_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    rule_name             VARCHAR(64)  NOT NULL,
    rule_details          JSONB,

    -- Kafka coordinates for debug / audit
    -- ("did the consumer reprocess this message?" -> trivially answerable)
    kafka_partition       INT,
    kafka_offset          BIGINT,

    consumer_processed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- Idempotency contract: a given transaction can only be flagged once per rule
    CONSTRAINT uq_streaming_alert_txn_rule UNIQUE (transaction_id, rule_name)
);

CREATE INDEX IF NOT EXISTS idx_alerts_name_orig  ON public.streaming_alerts(name_orig);
CREATE INDEX IF NOT EXISTS idx_alerts_flagged_at ON public.streaming_alerts(flagged_at);

COMMENT ON TABLE  public.streaming_alerts IS
    'Phase 4: one row per FLAG decision from streaming/consumer.py. Operational, not analytical.';
COMMENT ON COLUMN public.streaming_alerts.rule_details IS
    'JSONB evidence the rule used: recent_count, total_amount, time_window_seconds, threshold_n, threshold_t.';
COMMENT ON COLUMN public.streaming_alerts.kafka_partition IS
    'Source Kafka partition. Combined with kafka_offset, uniquely identifies the source message.';
