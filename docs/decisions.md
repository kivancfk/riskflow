# Architecture Decision Records

A running log of architectural decisions made during the build, in chronological
order. The README is the spec; this file captures *why* the spec looks the way
it does and *what changed* during implementation.

Format: each ADR is short — context, decision, consequences. No essays.

---

## ADR-001 — Airflow LocalExecutor (not Celery / Kubernetes)

**Date:** 2026-04-27
**Status:** Accepted

**Context.** The local stack must boot quickly on a portfolio reviewer's machine
with limited RAM. CeleryExecutor adds Redis + worker containers (~2GB RAM) and
~90 seconds to startup. KubernetesExecutor is overkill for a single-machine demo.

**Decision.** Use LocalExecutor — scheduler and webserver in one container.

**Consequences.**
- (+) Stack boots in ~30s instead of ~2m.
- (+) Simpler logs (single container).
- (−) Not a faithful representation of how Airflow runs in production.
- Mitigation: README explicitly notes that production would use CeleryExecutor
  or KubernetesExecutor.

---

## ADR-002 — SparkSubmitOperator over in-process PySpark

**Date:** 2026-04-27
**Status:** Accepted

**Context.** Airflow can run PySpark jobs two ways: (a) via SparkSubmitOperator
talking to a real Spark master, or (b) via PythonOperator with PySpark imported
directly into the worker. Option (b) is easier but conflates Airflow and Spark
and produces no Spark UI for those jobs.

**Decision.** Run a standalone Spark cluster (master + 1 worker) and submit jobs
via SparkSubmitOperator.

**Consequences.**
- (+) Real Spark UI screenshots possible — critical for Phase 5 performance writeup.
- (+) Closer to production patterns.
- (−) ~2GB more RAM for the cluster.
- (−) More moving parts to debug if something fails.

---

## ADR-003 — Kafka in KRaft mode (no Zookeeper)

**Date:** 2026-04-27
**Status:** Accepted

**Context.** Kafka 3.3+ supports KRaft mode, which removes the Zookeeper
dependency. Most outdated tutorials still use Zookeeper.

**Decision.** Run Kafka in KRaft mode.

**Consequences.**
- (+) One fewer container to run and explain.
- (+) Matches what new production deployments look like in 2026.
- (−) None significant for this scope.

---

## ADR-004 — Bronze partitioning by `load_date`, not `event_date`

**Date:** 2026-04-27
**Status:** Accepted

**Context.** PaySim has a `step` column representing event time. Bronze could be
partitioned by either the date the event occurred or the date the file was
processed. The two diverge if a daily file is reloaded.

**Decision.** Partition bronze by `load_date` — the date the partition was
written.

**Consequences.**
- (+) Re-running a daily ingest produces an idempotent overwrite.
- (+) Easy to see which partitions came from which DAG run.
- (−) Event-time analytics live in silver/gold and require a join through
  `step → date`, which is fine.

---

## ADR-005 — Two separate bronze locations for batch and streaming

**Date:** 2026-04-27
**Status:** Accepted (deliberate tradeoff)

**Context.** A unified bronze (Lambda or Kappa architecture) would be the
production-grade choice. It also takes much longer to build and debug.

**Decision.** Keep `bronze/` (batch) and `bronze_streaming/` (Kafka) physically
separate. Document the unified design in README §12 as a future improvement.

**Consequences.**
- (+) Each pipeline is independently runnable.
- (+) Architectural tradeoff is visible — interview talking point.
- (−) Downstream silver currently reads only from `bronze/`. The streaming
  branch is demonstrative, not load-bearing.

---

## ADR-006 — Phase 4 streaming uses kafka-python, not Spark Structured Streaming

**Date:** 2026-05-05
**Status:** Accepted

**Context.** Earlier roadmap and README diagrams showed Phase 4 as a Spark
Structured Streaming consumer reading from Kafka and writing to a
`bronze_streaming/` Parquet location, watermarked. As Phase 4 design work
progressed it became clear this was the wrong fit for the actual Phase 4 *goal*
(demonstrating real-time fraud-decisioning patterns) and for the available
*time* (4-day window). Spark Structured Streaming's state-store config,
checkpoint locations, and watermark mechanics are not relevant to demonstrating
fraud-detection logic, and would realistically consume 5–7 days including debugging.

**Decision.** Phase 4 uses `kafka-python` directly:
- `streaming/producer.py` replays silver Parquet to topic `riskflow.transactions`,
  with deterministic SHA-256-prefix `transaction_id`s and an opt-in `DEMO_BURST` mode.
- `streaming/consumer.py` reads the topic, maintains a bounded in-memory deque
  per customer, applies a pure-function velocity rule, and writes flagged events
  to `public.streaming_alerts` via `INSERT ... ON CONFLICT DO NOTHING`.
- Output is operational (`public.streaming_alerts`), not analytical
  (`bronze_streaming/` Parquet does not exist in Phase 4).

Spark Structured Streaming is documented as the production extension. The pure-
function rule (`streaming.fraud_rule.evaluate_velocity_rule`) is the only piece
that survives a future Spark/Flink rewrite unchanged.

**Consequences.**
- (+) Phase 4 ships in 4 days with a credible demo.
- (+) Producer/consumer are short, readable, debuggable (~150 lines each).
- (+) At-least-once Kafka delivery + idempotent sink (UNIQUE constraint on
  `(transaction_id, rule_name)`) = effectively-once outcomes.
- (+) Interview talking point: *"Python for clarity at this scale; in production
  I'd use Spark Structured Streaming or Flink for checkpointed state and
  horizontal scaling."*
- (−) State (per-customer deques) is lost on consumer restart. Acceptable for
  Phase 4; production would use RocksDB-backed state with checkpointing.
- (−) Single-process consumer cannot scale horizontally — irrelevant at our
  100 msg/sec demo rate, blocking at production throughput.

**Relationship to ADR-005.** ADR-005 is *not* superseded — it remains the
intended design for Phase 5+ when Spark Structured Streaming is reintroduced
and `bronze_streaming/` becomes load-bearing. ADR-006 narrows Phase 4's scope:
no `bronze_streaming/` in this phase, only `public.streaming_alerts`.

---

<!--
When making a new architectural decision during build, append a new ADR
above this line. Don't edit existing ADRs — superseded decisions should
be marked "Status: Superseded by ADR-XXX" rather than rewritten.
-->
