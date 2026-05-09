"""Kafka consumer: reads riskflow.transactions, applies the velocity rule,
writes flagged events to public.streaming_alerts.

Owns (per design §12):
  - per-customer transaction history (bounded deque)
  - Kafka consumer client (manual PER-MESSAGE offset commits, never
    batch-level commits that could over-commit on a mid-batch break)
  - Postgres connection (psycopg2, idempotent INSERT ... ON CONFLICT)
  - SIGTERM/SIGINT handlers for graceful shutdown

The fraud rule itself is a pure function in streaming.fraud_rule — this
module just owns the state and I/O around it.

Time semantics (design §4): processing-time. Each transaction is timestamped
at the moment the consumer receives it; the K-second window is wall-clock
relative to that. We do NOT use upstream `step` because PaySim step values
are hour-bucket integers, not real timestamps.

Delivery semantics: at-least-once via Kafka + idempotent sink via the
(transaction_id, rule_name) UNIQUE constraint = effectively-once outcomes.
A crash between Postgres commit and Kafka offset commit causes the message
to be replayed; `ON CONFLICT DO NOTHING` handles the duplicate.
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
from collections import defaultdict, deque
from typing import Deque, Dict

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

from streaming.config import StreamingConfig
from streaming.fraud_rule import Transaction, evaluate_velocity_rule

log = logging.getLogger("consumer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

_shutdown = False


def _handle_signal(signum: int, _frame) -> None:
    global _shutdown
    log.info("received signal %s, shutting down...", signum)
    _shutdown = True


INSERT_SQL = """
    INSERT INTO public.streaming_alerts (
        transaction_id, name_orig, transaction_amount, transaction_type,
        rule_name, rule_details, kafka_partition, kafka_offset
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id, rule_name) DO NOTHING;
"""


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    cfg = StreamingConfig.from_env()
    log.info(
        "config: bootstrap=%s topic=%s group=%s thresholds=N=%d K=%ds T=%.0f",
        cfg.kafka_bootstrap, cfg.topic, cfg.consumer_group,
        cfg.thresholds.n, cfg.thresholds.k_seconds, cfg.thresholds.t_amount,
    )

    consumer = KafkaConsumer(
        cfg.topic,
        bootstrap_servers=cfg.kafka_bootstrap,
        group_id=cfg.consumer_group,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        max_poll_records=100,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    pg = psycopg2.connect(cfg.pg_dsn)
    pg.autocommit = False

    # Per-customer state. Bounded deques prevent unbounded memory growth
    # (e.g., a long-running consumer seeing millions of unique customers).
    history: Dict[str, Deque[Transaction]] = defaultdict(
        lambda: deque(maxlen=cfg.history_max_per_customer)
    )

    processed = 0
    flagged = 0
    last_log = time.time()

    log.info("consumer started, polling for messages...")

    try:
        while not _shutdown:
            batch = consumer.poll(timeout_ms=cfg.poll_timeout_ms)
            if not batch:
                continue

            for tp, messages in batch.items():
                for msg in messages:
                    if _shutdown:
                        # Break out cleanly. Do NOT commit anything we
                        # haven't processed yet — the message at msg.offset
                        # has not been handled, so nothing to commit for it.
                        break

                    payload = msg.value
                    now = time.time()
                    txn = Transaction(
                        transaction_id=payload["transaction_id"],
                        name_orig=payload["name_orig"],
                        amount=float(payload["amount"]),
                        processing_time=now,
                    )

                    # Append to per-customer history, then filter to the
                    # K-second window. This is the consumer's job — the
                    # rule function does not look at processing_time.
                    history[txn.name_orig].append(txn)
                    cutoff = now - cfg.thresholds.k_seconds
                    recent = [
                        t for t in history[txn.name_orig]
                        if t.processing_time >= cutoff
                    ]

                    decision = evaluate_velocity_rule(recent, cfg.thresholds)

                    if decision.action == "FLAG":
                        with pg.cursor() as cur:
                            cur.execute(INSERT_SQL, (
                                txn.transaction_id,
                                txn.name_orig,
                                txn.amount,
                                payload.get("type"),
                                decision.rule_name,
                                Json(decision.evidence),
                                msg.partition,
                                msg.offset,
                            ))
                        # Commit Postgres BEFORE Kafka offset commit. If we
                        # crash between these, the message will be replayed
                        # and ON CONFLICT DO NOTHING handles the duplicate.
                        pg.commit()
                        flagged += 1
                        ev = decision.evidence or {}
                        log.info(
                            "FLAG name_orig=%s amount=%.2f recent_count=%d total=%.2f",
                            txn.name_orig, txn.amount,
                            ev.get("recent_count", 0),
                            ev.get("total_amount", 0),
                        )

                    # Commit THIS message's offset only — never a batched
                    # commit that could over-commit on a mid-batch break.
                    # OffsetAndMetadata wants offset+1 ("next offset to read").
                    consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})

                    processed += 1

                if _shutdown:
                    break

            if time.time() - last_log >= 10:
                log.info(
                    "processed=%d flagged=%d active_customers=%d",
                    processed, flagged, len(history),
                )
                last_log = time.time()
    finally:
        log.info("flushing and closing...")
        # Do NOT call consumer.commit() with no args here — it would
        # commit the current logical position, which poll() may have
        # advanced past unprocessed messages. Per-message commits above
        # are sufficient.
        try:
            consumer.close()
        except Exception as e:
            log.warning("consumer.close failed: %s", e)
        try:
            pg.close()
        except Exception as e:
            log.warning("pg.close failed: %s", e)
        log.info(
            "consumer shut down cleanly (processed=%d flagged=%d)",
            processed, flagged,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
