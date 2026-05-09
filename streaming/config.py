"""Streaming subsystem configuration.

Single source of truth for env-var → config mapping. Both producer.py and
consumer.py call `StreamingConfig.from_env()` at startup; tests instantiate
the dataclass directly so unit tests don't depend on env state.

Frozen dataclasses prevent accidental mutation mid-run.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class VelocityThresholds:
    """Thresholds for the velocity fraud rule.

    Strict-greater-than on both: a customer must have *more than* N
    transactions whose summed amount is *more than* T within the last
    K seconds (processing-time) to flag.
    """
    n: int = 5
    k_seconds: int = 60
    t_amount: float = 100_000.0


@dataclass(frozen=True)
class StreamingConfig:
    # Kafka
    kafka_bootstrap: str = "kafka:9092"
    topic: str = "riskflow.transactions"
    consumer_group: str = "riskflow-streaming-consumer"

    # Postgres
    pg_dsn: str = (
        "host=postgres port=5432 dbname=riskflow "
        "user=riskflow password=riskflow"
    )

    # Producer
    silver_path: str = "/opt/airflow/data/silver"
    msg_per_sec: float = 100.0
    demo_burst: bool = False
    burst_every_n: int = 500

    # Consumer
    history_max_per_customer: int = 100
    poll_timeout_ms: int = 1000

    # Rule
    thresholds: VelocityThresholds = field(default_factory=VelocityThresholds)

    @classmethod
    def from_env(cls) -> "StreamingConfig":
        return cls(
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic=os.getenv("KAFKA_TOPIC", "riskflow.transactions"),
            consumer_group=os.getenv("KAFKA_GROUP_ID", "riskflow-streaming-consumer"),
            pg_dsn=os.getenv(
                "PG_DSN",
                "host=postgres port=5432 dbname=riskflow user=riskflow password=riskflow",
            ),
            silver_path=os.getenv("SILVER_PATH", "/opt/airflow/data/silver"),
            msg_per_sec=float(os.getenv("MSG_PER_SEC", "100")),
            demo_burst=os.getenv("DEMO_BURST", "false").lower() == "true",
            burst_every_n=int(os.getenv("BURST_EVERY_N", "500")),
            history_max_per_customer=int(os.getenv("HISTORY_MAX_PER_CUSTOMER", "100")),
            poll_timeout_ms=int(os.getenv("POLL_TIMEOUT_MS", "1000")),
            thresholds=VelocityThresholds(
                n=int(os.getenv("VELOCITY_N", "5")),
                k_seconds=int(os.getenv("VELOCITY_K_SECONDS", "60")),
                t_amount=float(os.getenv("VELOCITY_T_AMOUNT", "100000")),
            ),
        )
