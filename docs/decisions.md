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

<!--
When making a new architectural decision during build, append a new ADR
above this line. Don't edit existing ADRs — superseded decisions should
be marked "Status: Superseded by ADR-XXX" rather than rewritten.
-->
