"""Integration tests for the streaming pipeline.

Marked `integration` and excluded from default `pytest` runs (see
pyproject.toml addopts). Run explicitly via:

    pytest -m integration
    # or:
    make test-streaming-integration

Heavy dependencies (`psycopg2`, `kafka-python`) are loaded via
`pytest.importorskip` so that if they're missing from the test runner
environment, this entire module is *skipped* during collection — it does
not cause unit tests to fail to collect.

Prerequisites (the docker stack must be up):
  - Kafka broker reachable at $KAFKA_BOOTSTRAP_SERVERS
  - Postgres reachable at $PG_DSN
  - The streaming_alerts table exists (`scripts/02_streaming_alerts_table.sql`)

Topic isolation: each test creates a fresh per-run topic via the Kafka
admin API and the spawned consumer subprocess subscribes only to that
topic. This isolates the test from the production `streaming-consumer`
service that may be running concurrently — without isolation, the
production consumer would also consume our test messages, accumulate
state for the test customer, and corrupt the replay-idempotency
assertion.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid

import pytest

# Skip the whole module if heavy deps are missing — this prevents
# `pytest -m 'not integration'` from failing to collect when the runner
# environment doesn't have these libraries installed.
psycopg2 = pytest.importorskip("psycopg2")
kafka = pytest.importorskip("kafka")

# Late-bound imports (safe after importorskip succeeded).
import json  # noqa: E402  (stdlib)
from kafka import KafkaProducer  # noqa: E402
from kafka.admin import KafkaAdminClient, NewTopic  # noqa: E402
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError  # noqa: E402

pytestmark = pytest.mark.integration

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
PG_DSN = os.getenv(
    "PG_DSN",
    "host=localhost port=5432 dbname=riskflow user=riskflow password=riskflow",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_conn():
    conn = psycopg2.connect(PG_DSN)
    yield conn
    conn.close()


@pytest.fixture
def unique_customer():
    """A customer name that no prior test or run has touched."""
    return f"C_ITEST_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def kafka_producer():
    p = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    yield p
    p.close()


@pytest.fixture
def isolated_topic():
    """Create a unique Kafka topic per test, drop it on teardown.

    Critical for isolating tests from the live `streaming-consumer`
    service: the production consumer subscribes only to
    `riskflow.transactions`, so test traffic on a per-test topic is
    invisible to it. Without this isolation, the live consumer sees
    test messages, accumulates per-customer state for the test customer,
    and the replay test's idempotency assertion fails because the live
    consumer flags messages that the test consumer correctly leaves alone.
    """
    topic = f"itest.streaming.{uuid.uuid4().hex[:8]}"
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="itest-admin",
    )
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    yield topic
    try:
        admin.delete_topics([topic])
    except (UnknownTopicOrPartitionError, Exception):
        pass
    admin.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _publish_burst(producer: KafkaProducer, topic: str, customer: str,
                   count: int, amount: float, id_prefix: str = "itest"):
    """Publish `count` transactions for `customer` to `topic` with deterministic IDs.

    Same `id_prefix` + same `customer` + same `count` → identical
    transaction_ids on every call. This is what lets the replay test
    exercise the (transaction_id, rule_name) UNIQUE constraint.
    """
    for i in range(count):
        payload = {
            "transaction_id": f"{id_prefix}_{customer}_{i:04d}",
            "step": 1_000 + i,
            "type": "TRANSFER",
            "amount": amount,
            "name_orig": customer,
            "name_dest": "DEST",
            "is_fraud": 0,
        }
        producer.send(topic, key=customer, value=payload)
    producer.flush()


def _count_alerts_for(pg_conn, customer: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM public.streaming_alerts WHERE name_orig = %s;",
            (customer,),
        )
        return cur.fetchone()[0]


def _start_consumer_subprocess(
    topic: str | None = None,
    group_id: str | None = None,
) -> subprocess.Popen:
    """Spawn the consumer as a subprocess.

    `topic` overrides KAFKA_TOPIC for this consumer. `group_id` lets the
    replay test simulate a real consumer restart by reusing the same
    group across two consumer instances. If either is None, a fresh
    per-run value is used.
    """
    env = os.environ.copy()
    env.setdefault("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP)
    env.setdefault("PG_DSN", PG_DSN)
    if topic:
        env["KAFKA_TOPIC"] = topic
    env["KAFKA_GROUP_ID"] = group_id or f"itest-{uuid.uuid4().hex[:8]}"
    env["VELOCITY_K_SECONDS"] = "60"
    env["VELOCITY_N"] = "5"
    env["VELOCITY_T_AMOUNT"] = "100000"

    return subprocess.Popen(
        [sys.executable, "-m", "streaming.consumer"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _stop_consumer(proc: subprocess.Popen, timeout: float = 15) -> None:
    """Send SIGTERM and wait. Falls back to SIGKILL if the consumer hangs."""
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _wait_for_alerts(pg_conn, customer: str, expected_min: int,
                     timeout_s: float = 30) -> int:
    """Poll until ≥ expected_min alerts exist for the customer, or timeout."""
    deadline = time.time() + timeout_s
    count = 0
    while time.time() < deadline:
        count = _count_alerts_for(pg_conn, customer)
        if count >= expected_min:
            return count
        time.sleep(1)
    return count


def _cleanup_alerts(pg_conn, customer: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.streaming_alerts WHERE name_orig = %s;",
            (customer,),
        )
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_alerts_table_supports_idempotent_insert(pg_conn, unique_customer):
    """The (transaction_id, rule_name) UNIQUE constraint enables the
    consumer's ON CONFLICT DO NOTHING pattern. Insert the same row twice;
    the second insert must be a no-op.

    Pure SQL — no Kafka, no consumer, no topic isolation needed.
    """
    txn_id = f"itest_idemp_{uuid.uuid4().hex[:16]}"
    insert = """
        INSERT INTO public.streaming_alerts (
            transaction_id, name_orig, transaction_amount, transaction_type,
            rule_name, rule_details, kafka_partition, kafka_offset
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
        ON CONFLICT (transaction_id, rule_name) DO NOTHING;
    """
    args = (
        txn_id, unique_customer, 1234.56, "TRANSFER",
        "velocity_breach", '{"recent_count": 6}', 0, 42,
    )
    try:
        with pg_conn.cursor() as cur:
            cur.execute(insert, args)
            cur.execute(insert, args)  # duplicate — should be silently ignored
        pg_conn.commit()

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM public.streaming_alerts WHERE transaction_id = %s;",
                (txn_id,),
            )
            assert cur.fetchone()[0] == 1, "duplicate insert should be a no-op"
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public.streaming_alerts WHERE transaction_id = %s;",
                (txn_id,),
            )
        pg_conn.commit()


def test_full_pipeline_publishes_and_flags(
    pg_conn, unique_customer, kafka_producer, isolated_topic,
):
    """End-to-end: publish 7 high-value transactions for one customer to
    an isolated topic, run the consumer, expect at least one alert.

    Topic isolation ensures the live `streaming-consumer` (subscribed to
    `riskflow.transactions`) never sees these messages — only our test
    consumer does.
    """
    consumer_proc = _start_consumer_subprocess(topic=isolated_topic)
    try:
        # Give the consumer time to subscribe before we publish — auto_offset_reset=latest
        # means anything published BEFORE subscription is invisible.
        time.sleep(3)
        _publish_burst(
            kafka_producer, isolated_topic, unique_customer,
            count=7, amount=25_000,
        )

        count = _wait_for_alerts(pg_conn, unique_customer, expected_min=1, timeout_s=30)
        assert count >= 1, f"expected ≥1 alert for {unique_customer}, got {count}"
    finally:
        _stop_consumer(consumer_proc)
        _cleanup_alerts(pg_conn, unique_customer)


def test_replay_after_restart_does_not_duplicate_alerts(
    pg_conn, unique_customer, kafka_producer, isolated_topic,
):
    """Real replay test: stop the consumer, restart with a fresh in-memory
    deque, then republish the same transaction_ids on the SAME isolated
    topic. With state cleared, the rule re-evaluates from scratch and
    reaches the same FLAG decisions on the same final messages — and
    ON CONFLICT prevents new rows.

    Topic isolation is critical here: without it, the live production
    consumer would see both the phase-1 and phase-2 publishes, accumulate
    14 deque entries for the test customer, and start flagging messages
    that previously passed — creating new alert rows because those msgs'
    transaction_ids had no prior FLAG row. That's a stateful-rule artifact
    misattributed to a sink-idempotency failure.
    """
    group_id = f"itest-replay-{uuid.uuid4().hex[:8]}"

    # Phase 1: first consumer instance, first publish
    consumer1 = _start_consumer_subprocess(topic=isolated_topic, group_id=group_id)
    try:
        time.sleep(3)
        _publish_burst(
            kafka_producer, isolated_topic, unique_customer,
            count=7, amount=25_000, id_prefix="replay",
        )
        first_count = _wait_for_alerts(pg_conn, unique_customer,
                                       expected_min=1, timeout_s=30)
        assert first_count >= 1, "first batch did not produce an alert"
    finally:
        _stop_consumer(consumer1)

    # Phase 2: NEW consumer instance (empty in-memory state), same group,
    # same transaction_ids on republish, same isolated topic.
    consumer2 = _start_consumer_subprocess(topic=isolated_topic, group_id=group_id)
    try:
        time.sleep(3)
        _publish_burst(
            kafka_producer, isolated_topic, unique_customer,
            count=7, amount=25_000, id_prefix="replay",
        )
        # Wait long enough for any new INSERTs to land. We do NOT assert
        # ≥1 — we assert "did not grow".
        time.sleep(10)
        second_count = _count_alerts_for(pg_conn, unique_customer)
        assert second_count == first_count, (
            f"replay must not increase alert count: "
            f"first={first_count} second={second_count}"
        )
    finally:
        _stop_consumer(consumer2)
        _cleanup_alerts(pg_conn, unique_customer)
