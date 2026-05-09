<div align="center">

# RiskFlow

**A payment-risk ELT platform demonstrating production data engineering practices on Adyen-style transaction data.**

![Status](https://img.shields.io/badge/status-in_development-yellow?style=for-the-badge)
![Target](https://img.shields.io/badge/target-Adyen_Data_Engineer-0abf53?style=for-the-badge)
![Ship Date](https://img.shields.io/badge/ship_date-Jun_1_2026-blue?style=for-the-badge)
![License](https://img.shields.io/badge/license-MIT-lightgrey?style=for-the-badge)

---

### Built with

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-4.x-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.x-FF694B?style=flat-square&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)

### Quality & testing

![Pytest](https://img.shields.io/badge/pytest-≥80%25_coverage-0A9EDC?style=flat-square&logo=pytest&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-data_quality-FF6310?style=flat-square)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CI-2088FF?style=flat-square&logo=githubactions&logoColor=white)
![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-FAB040?style=flat-square&logo=precommit&logoColor=white)

</div>

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Why this project is relevant to payment / risk data engineering](#2-why-this-project-is-relevant-to-payment--risk-data-engineering)
3. [Architecture](#3-architecture)
4. [Data Layers](#4-data-layers)
5. [Orchestration Design](#5-orchestration-design)
6. [Data Quality Strategy](#6-data-quality-strategy)
7. [Testing Strategy](#7-testing-strategy)
8. [Performance Optimization](#8-performance-optimization)
9. [Monitoring / SLA](#9-monitoring--sla)
10. [How to Run Locally](#10-how-to-run-locally)
11. [Future Improvements](#11-future-improvements)

---

## 1. Problem Statement

Payment processors generate millions of transaction events per day across many merchants, currencies, and channels. Risk, compliance, and product teams need this data delivered in three different shapes:

- **Fresh** — for real-time fraud signals and authorization-rate monitoring.
- **Reliable** — typed, deduplicated, validated, and reconciled, with auditable lineage.
- **Modeled** — pre-aggregated into business-ready marts that analysts can query without rewriting joins.

RiskFlow simulates the data-platform layer of a payments company: it ingests transaction events from a synthetic but realistic source (PaySim), processes them through a medallion architecture (bronze → silver → gold), validates them at every layer, and serves analytical marts to downstream consumers. It is designed to demonstrate the engineering practices a payments-domain data team would expect, not to be a production system.

---

## 2. Why this project is relevant to payment / risk data engineering

Adyen's data teams (Protect, Platform Risk, Identity & Risk Intelligence, Compliance Data, Transaction & Ledger Platform, Authorization Optimisation) all share a common substrate: high-volume transactional events that must flow through reliable, observable, performant pipelines into both analytical stores and ML feature layers.

RiskFlow mirrors this substrate in miniature:

- **Domain shape** — PaySim provides labeled fraud transactions with sender/receiver, type, amount, balances. The schema and access patterns rhyme with real card-payment data.
- **Engineering shape** — incremental ingestion, schema enforcement, data quality gates, monitoring tables, performance tuning, and reproducible local execution.
- **Architectural shape** — medallion layers, batch + streaming co-existence, dbt for analytical modeling, Pytest + Great Expectations for trust.

This is the substrate. The marts are what a fraud or risk team would actually query.

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE                                          │
│   PaySim CSV (split into daily partitions: day_01.csv … day_30.csv)          │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                            ┌───────────────┐
                            │  Airflow DAG  │
                            │ daily_ingest  │
                            └───────┬───────┘
                                    │
                                    ▼
                       ┌───────────────────────┐
                       │ BRONZE                │
                       │ Parquet, partitioned  │
                       │ by load_date          │
                       └───────────┬───────────┘
                                   │
                                   ▼
            ┌───────────────────────────────────────────────────┐
            │ Great Expectations validation gate                │
            │   - schema, types, ranges, duplicates, nulls      │
            │   - label domain, row-count parity bronze→silver  │
            │ Failed rows → `failed_records` table              │
            └───────────────────────┬───────────────────────────┘
                                    │
                                    ▼
                       ┌───────────────────────┐
                       │ SILVER                │
                       │ PySpark transforms    │
                       │ Parquet, by load_date │
                       └────┬─────────────┬────┘
                            │             │
       ┌────────────────────┘             └──────────────────────┐
       │ (analytical, batch)                (operational, Phase 4)│
       ▼                                                          ▼
 ┌───────────────────┐                     ┌─────────────────────────────────┐
 │ GOLD (dbt)        │                     │ streaming/producer.py           │
 │   4 marts         │                     │   replays silver → Kafka        │
 │   tests + lineage │                     └────────────────┬────────────────┘
 └─────────┬─────────┘                                      │
           │                                                ▼
           ▼                                     ┌────────────────────────┐
 ┌──────────────────┐                            │ Kafka topic            │
 │  PostgreSQL      │                            │ riskflow.transactions  │
 │  (gold marts)    │                            │ 3 partitions, KRaft    │
 └────────┬─────────┘                            └────────────┬───────────┘
          │                                                   │
          ▼                                                   ▼
 ┌──────────────────┐                            ┌─────────────────────────────────┐
 │  Streamlit       │                            │ streaming/consumer.py           │
 │  dashboard       │                            │   deque per customer +          │
 └──────────────────┘                            │   pure-function velocity rule + │
                                                 │   idempotent INSERT (ON CONFL)  │
                                                 └────────────────┬────────────────┘
                                                                  │
                                                                  ▼
                                                 ┌────────────────────────────┐
                                                 │ PostgreSQL                 │
                                                 │   public.streaming_alerts  │
                                                 │   (operational sink)       │
                                                 └────────────────────────────┘

OBSERVABILITY (cross-cutting):
   - `pipeline_runs` table     — one row per DAG run, status, durations, row counts
   - `failed_records` table    — rows that failed GE validation, with rule name + reason
   - `streaming_alerts` table  — one row per FLAG decision; Kafka partition + offset recorded
   - Airflow task logs, Spark UI screenshots in /docs/performance.md
   - Kafka UI at http://localhost:8090 (consumer lag, topic offsets)
```

Batch and streaming serve different purposes: the batch path (Airflow + Spark + dbt) produces analytical marts in `gold` for dashboards and ML feature engineering; the streaming path (Python + Kafka) produces operational alerts in `public.streaming_alerts` for sub-second fraud-blocking decisions. The two paths share the silver layer as a contract — the streaming producer replays validated silver records — but otherwise run independently. Phase 4 uses kafka-python for clarity at this scale; production would use Spark Structured Streaming or Flink for checkpointed state, horizontal scaling, and exactly-once semantics (see [ADR-006](docs/decisions.md) in `docs/decisions.md`).

---

## 4. Data Layers

### Source — PaySim
A public synthetic mobile-money fraud dataset. Chosen because it is large enough to be meaningful, has a fraud label, and can be partitioned into daily files to simulate incremental loads. ~6M rows in the full dataset; this project uses the first 30 days.

### Bronze — Raw, Faithful, Immutable
- Format: Parquet
- Partitioning: by `load_date` (the date the file was processed, not the event date — a deliberate choice so re-runs are idempotent)
- Schema: identical to source CSV, plus `_load_ts`, `_source_file` lineage columns
- No transformations beyond format conversion

### Silver — Cleaned, Conformed, Trusted
- Format: Parquet
- Partitioning: by `load_date`
- Transformations:
  - Type casting (amounts → DECIMAL, timestamps → TIMESTAMP)
  - Deduplication on `(transaction_id, step)`
  - `is_fraud` enforced to `{0, 1}` (rejected rows land in `failed_records`)
  - Account-side enrichment (sender/receiver type derived from name prefix)
  - Currency normalization placeholder (PaySim is single-currency; real systems would FX-normalize here)
- Validated by Great Expectations before being made available downstream

### Gold — Modeled, Aggregated, Business-Ready
Owned and built by dbt. Four marts:

| Mart | Grain | Purpose |
|---|---|---|
| `gold_daily_fraud_summary` | day × transaction_type | Fraud rate, count, amount-weighted fraud rate. Feeds risk dashboards. |
| `gold_customer_risk_features` | customer | Velocity, recency, frequency, fraud-history features. Feeds ML scoring. |
| `gold_transaction_velocity_features` | customer × hour | Rolling-window transaction counts and amounts. Feeds real-time risk models. |
| `gold_pipeline_quality_summary` | day | Row counts, validation pass-rate, freshness — populated from `pipeline_runs` and `failed_records`. |

All marts have dbt tests (`unique`, `not_null`, `accepted_values`, custom range tests) and appear in the dbt lineage graph.

### Streaming output — Operational, not Analytical
The Phase 4 streaming path produces a single Postgres table, `public.streaming_alerts`, distinct from the gold marts. It exists to support real-time fraud-blocking decisions, not analytical queries — one row per FLAG decision from the velocity rule, with the source Kafka partition and offset recorded for debugging and audit. The schema is intentionally narrow:

| Column | Purpose |
|---|---|
| `transaction_id`, `name_orig`, `transaction_amount`, `transaction_type` | Source-message identity |
| `rule_name`, `rule_details` (JSONB) | Which rule fired and the evidence dict |
| `kafka_partition`, `kafka_offset` | Source Kafka coordinates — answers "did the consumer reprocess this?" |
| `flagged_at`, `consumer_processed_at` | Two timestamps because they can diverge during replay |

A `UNIQUE (transaction_id, rule_name)` constraint plus the consumer's `INSERT ... ON CONFLICT DO NOTHING` makes alert writes idempotent on top of Kafka's at-least-once delivery. See §5 for the full streaming architecture.

---

## 5. Orchestration Design

**Airflow** runs the batch path. **Streaming runs as standalone Docker services**, intentionally not Airflow DAGs — see below.

### `riskflow_daily_ingest`
- Schedule: `@daily` (in production); manually triggered per partition during development
- Tasks:
  1. `check_source_file_exists` — sensor
  2. `ingest_to_bronze` — PySpark job, reads `day_X.csv`, writes Parquet
  3. `validate_bronze` — Great Expectations checkpoint
  4. `transform_to_silver` — PySpark job, applies cleaning + dedup
  5. `validate_silver` — Great Expectations checkpoint
  6. `dbt_run_gold` — `dbt run --select tag:gold`
  7. `dbt_test_gold` — `dbt test --select tag:gold`
  8. `record_pipeline_run` — writes a row to `pipeline_runs`

Failure of any validation task halts the DAG and logs to `failed_records`. The DAG is **not** retry-on-failure for validation steps — bad data should not be silently retried.

### Streaming services (not Airflow-orchestrated)

The Phase 4 streaming producer and consumer are long-running Docker services, not Airflow DAGs. This is deliberate: batch jobs are DAG-shaped (start, run, finish) while streaming jobs are always-on. Conflating them would muddy a clear architectural distinction and force a long-running Airflow task to occupy a worker slot indefinitely. The two services are managed via Make targets:

```bash
make streaming-init-topic   # idempotent topic creation (Phase 4 owns its topic)
make streaming-up           # bring up producer + consumer (auto-resets streaming_alerts unless KEEP=1)
make streaming-down         # stop both
make streaming-watch-db     # live alert count in Postgres
make streaming-watch-log    # tail consumer logs
make streaming-reset        # truncate streaming_alerts
```

### Streaming architecture (Phase 4)

**Path:** silver Parquet → `streaming/producer.py` → Kafka topic `riskflow.transactions` → `streaming/consumer.py` → `public.streaming_alerts`.

**Rule.** Velocity-based, computed in-memory per customer. Flag if a customer has more than N=5 transactions whose summed amount exceeds T=100,000 within the last K=60 seconds (processing-time). The rule itself is a pure function in `streaming/fraud_rule.py` — the consumer owns the per-customer deques, the rule just decides FLAG vs PASS given an already-windowed list. This split keeps the rule trivially unit-testable and means a future migration to Spark Structured Streaming or Flink only has to replace the windowing/state machinery; the rule function survives unchanged.

**Time semantics.** Processing-time (wall-clock at consumer-receive). PaySim `step` values are hour buckets, not real timestamps; processing-time is the honest choice for replayed synthetic data. A real Adyen platform would use event-time with watermarks because the upstream timestamp is meaningful.

**Delivery semantics.** At-least-once via Kafka (manual per-message offset commits, commit-after-side-effect) plus an idempotent sink via the `(transaction_id, rule_name)` UNIQUE constraint = effectively-once outcomes. A crash between Postgres commit and Kafka offset commit causes replay; `ON CONFLICT DO NOTHING` handles the duplicate. Transaction IDs are deterministic SHA-256 prefixes of source fields so replays produce the same IDs (a random UUID would break the contract).

**State.** In-memory bounded deques (max 100 entries per customer). Lost on container restart. Acceptable for Phase 4; production would use RocksDB-backed Spark/Flink state with checkpointing — see [ADR-006](docs/decisions.md).

**Demo aid.** `DEMO_BURST=true` (the default in `docker-compose.yml`) injects 6 high-value transactions for synthetic customer `C_DEMO_BURST` every 500 rows, guaranteeing the rule fires within ~60 seconds of `make streaming-up`. Documented as a demo trigger, not natural data; the synthetic customer is excluded from any analytical use.

---

## 6. Data Quality Strategy

Three layers of defense, intentionally redundant:

**Layer 1 — Pytest (unit tests on transformation logic)**
- Pure-function tests on PySpark transformations using small in-memory DataFrames
- Pure-function tests on the Phase 4 fraud rule (`streaming/tests/test_fraud_rule.py`)
- Covers edge cases: empty input, all-null columns, duplicate keys, malformed types, threshold boundaries
- Runs in CI on every commit

**Layer 2 — Great Expectations (data tests on actual data)**
- Bronze checkpoint: schema, expected columns, row count > 0
- Silver checkpoint: amount ≥ 0, `is_fraud` ∈ {0, 1}, no duplicate `transaction_id`, row-count parity vs. bronze (within tolerance for dropped invalid rows)
- Failed rows are quarantined in `failed_records` with the rule name that fired

**Layer 3 — dbt tests (model contracts)**
- `unique`, `not_null` on primary keys of every gold mart
- `accepted_values` on categorical columns
- Custom relationship tests between marts

The redundancy is deliberate: Pytest catches logic bugs before deployment; GE catches data-shape surprises in production; dbt tests guarantee mart contracts. Each layer is cheap and each fails differently.

---

## 7. Testing Strategy

- **Unit tests** — `tests/unit/` covers transformation functions; `streaming/tests/test_fraud_rule.py` covers the Phase 4 rule. Pure functions, milliseconds to run.
- **Integration tests** — `tests/integration/` covers end-to-end DAG runs; `streaming/tests/test_streaming_integration.py` covers producer→Kafka→consumer→Postgres with **per-test topic isolation** (each test creates a unique topic via `KafkaAdminClient` so the live consumer service running concurrently never sees test traffic).
- **Marker split** — integration tests are marked `integration` and skipped by default. `make test-unit` runs unit only (~1 minute); `make test-streaming-integration` runs the streaming end-to-end suite explicitly.
- **Coverage target**: ≥80% on the `transformations/` and `streaming/` modules. No target on glue code.
- **Fixtures**: `tests/fixtures/paysim_sample.csv` — 1,000 rows handpicked to include duplicates, nulls, fraud cases, and edge values.
- **CI**: GitHub Actions runs Pytest + dbt parse + GE schema validation on every push.

---

## 8. Performance Optimization

A dedicated section because performance work is the single most demonstrable aspect of distributed-data engineering. Detailed writeup in [`docs/performance.md`](docs/performance.md). Highlights:

### Pandas vs. PySpark baseline
The same silver-transformation logic implemented in pandas and in PySpark, run on 30 days of PaySim. Documented timings, memory, and the inflection point where pandas becomes infeasible.

### Spark optimization passes
Three before/after demonstrations, each with Spark UI screenshots:

1. **Partitioning** — naïve unpartitioned read vs. `partitionBy("load_date")`. Shows partition pruning in the physical plan.
2. **Broadcast joins** — silver ⨝ small dimension table. Default sort-merge join vs. `broadcast()` hint. Stage timings before/after.
3. **Adaptive Query Execution + skew handling** — deliberately introduce a skewed key. Show the long-tail task. Enable AQE skew join. Show the rebalanced stage.

Each subsection documents: (a) the symptom, (b) the diagnosis from Spark UI, (c) the fix, (d) the measured improvement.

---

## 9. Monitoring / SLA

### `pipeline_runs` table

| Column | Type | Description |
|---|---|---|
| `run_id` | UUID | Airflow run identifier |
| `dag_id` | TEXT | DAG name |
| `partition_date` | DATE | Logical partition being processed |
| `status` | TEXT | `success` / `failed` / `validation_failed` |
| `started_at` | TIMESTAMP | |
| `ended_at` | TIMESTAMP | |
| `duration_seconds` | INT | |
| `bronze_row_count` | BIGINT | |
| `silver_row_count` | BIGINT | |
| `failed_row_count` | BIGINT | |

### `failed_records` table

| Column | Type | Description |
|---|---|---|
| `run_id` | UUID | FK to `pipeline_runs` |
| `transaction_id` | TEXT | Original row identifier |
| `rule_name` | TEXT | GE expectation that failed |
| `reason` | TEXT | Human-readable failure |
| `raw_row` | JSONB | Full original row for debugging |

### `streaming_alerts` table (Phase 4)

See §4 for the full schema. Operationally, the columns that matter for monitoring are `flagged_at` (latency from event to alert), `kafka_partition` + `kafka_offset` (replay debugging), and `rule_details` (a JSONB evidence dict including `recent_count` and `total_amount` at the moment the rule fired). Consumer lag is visible in Kafka UI at port 8090.

### SLA assumptions (documented, not enforced in this project)

- **Freshness (batch)**: Daily partition for `day_X` available in gold by `day_X + 1` 06:00 UTC.
- **Freshness (streaming)**: Alert latency from Kafka publish to `streaming_alerts` row visible < 1 second p99 at the demo's 100 msg/s rate.
- **Completeness**: ≥99% of source rows reach silver. Below 99% → page on-call.
- **Validation pass rate**: ≥99.5% of silver rows pass all GE expectations.

These are written explicitly to make the operational thinking visible. A real platform would emit metrics to Prometheus / Datadog and alert against these thresholds.

---

## 10. How to Run Locally

### Prerequisites
- Docker Desktop (≥8GB allocated)
- ~5GB free disk for Parquet output
- The PaySim CSV downloaded to `data/raw/paysim.csv` (see `data/README.md` for the source link)

### Bring up the stack

```bash
git clone https://github.com/kivancfk/riskflow.git
cd riskflow
make split-data        # splits paysim.csv into 30 daily partitions
docker compose up -d   # boots postgres, airflow, spark, kafka, kafka-ui
make init              # creates Airflow connections, dbt profiles, GE suites
```

### Run the batch pipeline for one partition

```bash
make run-day DAY=01    # triggers riskflow_daily_ingest for day_01.csv
make logs              # tails the DAG logs
```

Airflow UI: http://localhost:8080 (admin / admin)
Streamlit dashboard: http://localhost:8501
Kafka UI: http://localhost:8090

### Run the Phase 4 streaming pipeline

```bash
# Apply the Phase 4 DDL (idempotent)
docker compose exec -T postgres psql -U riskflow -d riskflow \
  < scripts/02_streaming_alerts_table.sql

# Bring up the streaming services
make streaming-init-topic   # create the Kafka topic (idempotent)
make streaming-up           # start producer + consumer
```

Open two more terminals to watch:

```bash
make streaming-watch-db     # live alert count in streaming_alerts
make streaming-watch-log    # tail consumer logs
```

With `DEMO_BURST=true` (default), an alert appears in `streaming_alerts` within ~60 seconds.

### Run the test suite

```bash
make test-unit                    # fast, default — unit tests only
make test-streaming-integration   # producer→Kafka→consumer→Postgres end-to-end
make test-all                     # both, sequentially (CI)
```

### Tear down

```bash
make streaming-down
docker compose down -v
```

---

## 11. Future Improvements

What this project deliberately does not include, and what shipping it would require:

- **Spark Structured Streaming consumer** to replace the Phase 4 Python consumer — adds checkpointed state (RocksDB), horizontal scaling, and exactly-once semantics. The pure-function rule in `streaming/fraud_rule.py` is designed to survive this migration unchanged. See [ADR-006](docs/decisions.md).
- **Unified batch + streaming bronze** via Delta Lake or Apache Iceberg, with a single source-of-truth for downstream silver. Currently the streaming path replays silver rather than producing its own bronze; a unified design would close the loop.
- **Schema migrations** — currently the streaming-alerts DDL is `CREATE TABLE IF NOT EXISTS`, which silently no-ops on schema drift. Production needs Alembic / Sqitch / versioned SQL migrations.
- **Schema evolution handling** — currently assumes a fixed source schema. Real systems need additive-column-tolerant ingestion.
- **MLflow-tracked fraud model** scoring against `gold_customer_risk_features` and writing predictions back to a serving table.
- **Real cloud deployment** — currently Docker Compose only. Production would target Spark on Kubernetes (or EMR/Databricks) with managed Airflow (MWAA / Cloud Composer) and a real warehouse (Snowflake / BigQuery).
- **Lineage metadata** — OpenLineage emission from Airflow + Spark to a Marquez or DataHub instance.
- **CDC ingestion** — Debezium → Kafka for upstream operational databases.

---

<div align="center">

### Author

**Kıvanç Filizci**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-kivancfilizci-0A66C2?style=flat-square&logo=linkedin&logoColor=white)](https://linkedin.com/in/kivancfilizci)
[![GitHub](https://img.shields.io/badge/GitHub-kivancfk-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/kivancfk)
[![HackerRank](https://img.shields.io/badge/HackerRank-kivancfk-2EC866?style=flat-square&logo=hackerrank&logoColor=white)](https://hackerrank.com/kivancfk)

*Built April–June 2026 as a portfolio project for data-engineering roles in payments and fintech.*

</div>
