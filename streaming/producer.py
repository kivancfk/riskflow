"""Kafka producer: replays silver Parquet to the riskflow.transactions topic.

Reads from data/silver/load_date=YYYY-MM-DD/ partitions, sorts each partition
by (name_orig, step) so a customer's transactions cluster together in the
stream (design §13), and publishes one JSON message per row at a configurable
rate (default 100 msg/sec).

Loops forever (design Q3) so the demo can run as long as needed; logs
'🔁 starting cycle N' at each loop boundary so the operator can see it
hasn't stalled.

DEMO_BURST=true (design §6) injects 6 high-value transactions for synthetic
customer C_DEMO_BURST every BURST_EVERY_N rows, guaranteeing the velocity
rule fires within 60 seconds of startup.

transaction_id is a deterministic SHA-256 prefix (design §9). Replaying the
same source rows produces the same IDs, so consumer-side ON CONFLICT DO NOTHING
prevents duplicate alerts on replay.
"""
from __future__ import annotations

import hashlib
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Iterator

import pyarrow.dataset as ds
from kafka import KafkaProducer

from streaming.config import StreamingConfig

log = logging.getLogger("producer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

_shutdown = False


def _handle_signal(signum: int, _frame) -> None:
    global _shutdown
    log.info("received signal %s, shutting down...", signum)
    _shutdown = True


# ---------------------------------------------------------------------------
# Pure helpers — small enough to be testable as units if we ever want to.
# ---------------------------------------------------------------------------

def make_transaction_id(
    source_file: str,
    row_index: int,
    step: int,
    name_orig: str,
    name_dest: str,
    amount: float,
) -> str:
    """Deterministic 32-char hex prefix of SHA-256 of the source fields.

    See design §9. Replaying the same row produces the same ID; combined
    with the consumer's `INSERT ... ON CONFLICT (transaction_id, rule_name)
    DO NOTHING`, this gives effectively-once outcomes on top of Kafka's
    at-least-once delivery.

    32 hex chars = 128 bits — well above the collision floor for our scale.
    Stored in a VARCHAR(64) column, leaving room for future ID formats
    without a schema migration.
    """
    payload = f"{source_file}|{row_index}|{step}|{name_orig}|{name_dest}|{amount}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def make_burst_messages(burst_seq: int) -> list[dict]:
    """Generate 6 high-value transactions for the synthetic burst customer.

    `burst_seq` makes IDs deterministic *within* a producer run. Across
    restarts they diverge — fine, the burst customer is synthetic and is
    explicitly excluded from any analytical use.
    """
    base_step = 10_000 + burst_seq * 10
    return [
        {
            "source_file": f"_demo_burst_{burst_seq}",
            "row_index": i,
            "step": base_step + i,
            "type": "TRANSFER",
            "amount": 30_000.0 + i * 1_000,
            "name_orig": "C_DEMO_BURST",
            "name_dest": "C_DEMO_BURST_DEST",
            "is_fraud": 0,
        }
        for i in range(6)
    ]


def to_kafka_payload(row: dict) -> dict:
    """Wrap a silver row with a deterministic transaction_id."""
    txn_id = make_transaction_id(
        source_file=row["source_file"],
        row_index=row["row_index"],
        step=row["step"],
        name_orig=row["name_orig"],
        name_dest=row["name_dest"],
        amount=row["amount"],
    )
    return {
        "transaction_id": txn_id,
        "step": row["step"],
        "type": row["type"],
        "amount": row["amount"],
        "name_orig": row["name_orig"],
        "name_dest": row["name_dest"],
        "is_fraud": row["is_fraud"],
    }


# ---------------------------------------------------------------------------
# Silver reader
# ---------------------------------------------------------------------------

def iter_silver_rows(silver_path: str) -> Iterator[dict]:
    """Yield rows from all silver partitions, sorted by (name_orig, step) per partition.

    Each load_date partition is loaded into memory for the sort. Silver
    day-partitions are ~400k rows / ~30MB — trivial.
    """
    silver_dir = Path(silver_path)
    if not silver_dir.exists():
        raise FileNotFoundError(f"silver path not found: {silver_path}")

    partitions = sorted(
        p for p in silver_dir.iterdir()
        if p.is_dir() and p.name.startswith("load_date=")
    )
    if not partitions:
        raise RuntimeError(f"no load_date partitions found under {silver_path}")

    log.info("found %d silver partitions", len(partitions))

    for part in partitions:
        log.info("reading partition %s", part.name)
        dataset = ds.dataset(str(part), format="parquet")
        df = dataset.to_table().to_pandas()
        # Silver currently inherits PaySim's camelCase for these two columns.
        # rename() silently skips missing keys, so this is a no-op when/if
        # Phase 2's silver writer is normalized later. See ADR-006 follow-ups.
        df = df.rename(columns={
            "nameOrig": "name_orig",
            "nameDest": "name_dest",
        })
        df = df.sort_values(["name_orig", "step"], kind="mergesort")
        source_file = part.name

        for row_index, row in enumerate(df.itertuples(index=False)):
            yield {
                "source_file": source_file,
                "row_index": row_index,
                "step": int(row.step),
                "type": str(getattr(row, "type", "TRANSFER")),
                "amount": float(row.amount),
                "name_orig": str(row.name_orig),
                "name_dest": str(getattr(row, "name_dest", "")),
                "is_fraud": int(getattr(row, "is_fraud", 0)),
            }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> int:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    cfg = StreamingConfig.from_env()
    log.info(
        "config: bootstrap=%s topic=%s rate=%s/s demo_burst=%s burst_every=%d",
        cfg.kafka_bootstrap, cfg.topic, cfg.msg_per_sec,
        cfg.demo_burst, cfg.burst_every_n,
    )

    producer = KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=10,
        acks=1,
    )

    sleep_between = 1.0 / cfg.msg_per_sec if cfg.msg_per_sec > 0 else 0.0
    cycle = 0
    burst_seq = 0

    try:
        while not _shutdown:
            cycle += 1
            log.info("🔁 starting cycle %d (replaying silver from start)", cycle)
            rows_in_cycle = 0

            for row in iter_silver_rows(cfg.silver_path):
                if _shutdown:
                    break

                payload = to_kafka_payload(row)
                # Key by name_orig so all messages for a customer go to the
                # same Kafka partition — preserves per-customer ordering.
                producer.send(cfg.topic, key=payload["name_orig"], value=payload)
                rows_in_cycle += 1

                if cfg.demo_burst and rows_in_cycle % cfg.burst_every_n == 0:
                    burst_seq += 1
                    for r in make_burst_messages(burst_seq):
                        bp = to_kafka_payload(r)
                        producer.send(cfg.topic, key=bp["name_orig"], value=bp)
                    log.info(
                        "🔥 demo burst injected (cycle=%d row=%d burst_seq=%d)",
                        cycle, rows_in_cycle, burst_seq,
                    )

                if rows_in_cycle % 1_000 == 0:
                    log.info("cycle=%d published=%d", cycle, rows_in_cycle)

                if sleep_between > 0:
                    time.sleep(sleep_between)

            log.info("cycle %d complete: published %d rows", cycle, rows_in_cycle)
    finally:
        log.info("flushing producer...")
        try:
            producer.flush(timeout=10)
        except Exception as e:
            log.warning("flush failed: %s", e)
        producer.close()
        log.info("producer shut down cleanly")

    return 0


if __name__ == "__main__":
    sys.exit(main())
