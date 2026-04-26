# RiskFlow

**A payment-risk ELT platform demonstrating production data engineering practices on Adyen-style transaction data.**

> Built with Airflow, PySpark, Kafka, dbt, PostgreSQL, Docker, Pytest, and Great Expectations.

---

## 1. Problem Statement

Payment processors generate millions of transaction events per day across many merchants, currencies, and channels. Risk, compliance, and product teams need this data delivered in three different shapes:

- **Fresh** — for real-time fraud signals and authorization-rate monitoring.
- **Reliable** — typed, deduplicated, validated, and reconciled, with auditable lineage.
- **Modeled** — pre-aggregated into business-ready marts that analysts can query without rewriting joins.

RiskFlow simulates the data-platform layer of a payments company: it ingests transaction events from a synthetic but realistic source (PaySim), processes them through a medallion architecture (bronze → silver → gold), validates them at every layer, and serves analytical marts to downstream consumers. It is designed to demonstrate the engineering practices a payments-domain data team would expect, not to be a production system.

---

## 2. Why This Project Is Relevant to Payment / Risk Data Engineering

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
│                              SOURCES                                         │
│   PaySim CSV (split into daily partitions: day_01.csv … day_30.csv)          │
│   Kafka topic `transactions.live` (held-out slice replayed as a stream)      │
└──────────────────────────────────────────────────────────────────────────────┘
                │                                          │
                ▼                                          ▼
        ┌───────────────┐                          ┌───────────────────┐
        │  Airflow DAG  │                          │ Spark Structured  │
        │ daily_ingest  │                          │ Streaming         │
        └───────┬───────┘                          └─────────┬─────────┘
                │                                            │
                ▼                                            ▼
        ┌───────────────────────┐              ┌──────────────────────┐
        │ BRONZE (batch)        │              │ BRONZE (streaming)   │
        │ Parquet, partitioned  │              │ Parquet, append-only │
        │ by load_date          │              │ partitioned by hour  │
        └───────────┬───────────┘              └──────────────────────┘
                    │
                    ▼
        ┌───────────────────────────────────────────────────┐
        │ Great Expectations validation gate                │
        │   - schema, types, ranges                         │
        │   - duplicates, nulls, label domain               │
        │   - row-count parity bronze → silver              │
        │ Failed rows → `failed_records` table              │
        └───────────────────────┬───────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────────┐
        │ SILVER                                            │
        │ PySpark transformations                           │
        │   - typing, dedup, currency normalization         │
        │   - account-side enrichment                       │
        │   - is_fraud cast to {0,1}                        │
        │ Parquet, partitioned by load_date                 │
        └───────────────────────┬───────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────────┐
        │ GOLD (dbt)                                        │
        │   gold_daily_fraud_summary                        │
        │   gold_customer_risk_features                     │
        │   gold_transaction_velocity_features              │
        │   gold_pipeline_quality_summary                   │
        │ dbt tests + dbt docs (lineage)                    │
        └───────────────────────┬───────────────────────────┘
                                │
                                ▼
                       ┌────────────────┐
                       │  PostgreSQL    │
                       │  (serving)     │
                       └────────┬───────┘
                                │
                                ▼
                       ┌────────────────┐
                       │  Streamlit     │
                       │  dashboard     │
                       └────────────────┘

OBSERVABILITY (cross-cutting):
   - `pipeline_runs` table: one row per DAG run, status, durations, row counts
   - `failed_records` table: rows that failed GE validation, with rule name + reason
   - Airflow task logs
   - Spark UI screenshots in /docs/performance.md
```

The batch and streaming pipelines write to **separate** bronze locations (`bronze/` and `bronze_streaming/`). In a real system these would be unified via a Lambda or Kappa architecture; here they are kept independent so each pipeline is independently runnable and the architectural tradeoff is visible.

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

---

## 5. Orchestration Design

**Airflow** runs the batch path. Two DAGs:

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

### `riskflow_streaming_consume`
A standalone Spark Structured Streaming job (run as a long-lived Airflow task or independently). Consumes `transactions.live`, writes to `bronze_streaming/`. Watermarked at 10 minutes. Documented but not deeply integrated.

---

## 6. Data Quality Strategy

Three layers of defense, intentionally redundant:

**Layer 1 — Pytest (unit tests on transformation logic)**
- Pure-function tests on PySpark transformations using small in-memory DataFrames
- Covers edge cases: empty input, all-null columns, duplicate keys, malformed types
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

- **Pytest** — `tests/unit/` covers transformation functions, `tests/integration/` covers end-to-end DAG runs against a fixture dataset.
- **Coverage target**: ≥80% on the `transformations/` module. No target on glue code.
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

### SLA assumptions (documented, not enforced in this project)

- **Freshness**: Daily partition for `day_X` available in gold by `day_X + 1` 06:00 UTC.
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
docker compose up -d   # boots postgres, airflow, spark, kafka
make init              # creates Airflow connections, dbt profiles, GE suites
```

### Run the batch pipeline for one partition

```bash
make run-day DAY=01    # triggers riskflow_daily_ingest for day_01.csv
make logs              # tails the DAG logs
```

Airflow UI: http://localhost:8080 (admin / admin)
Streamlit dashboard: http://localhost:8501

### Run the streaming pipeline

```bash
make run-streaming     # starts the Kafka producer + Structured Streaming consumer
```

### Run the test suite

```bash
make test              # pytest + dbt test
```

### Tear down

```bash
docker compose down -v
```

---

## 11. Future Improvements

What this project deliberately does not include, and what shipping it would require:

- **Unified batch + streaming bronze** via Delta Lake or Apache Iceberg, with a single source-of-truth for downstream silver. Currently kept separate for architectural clarity.
- **Schema evolution handling** — currently assumes a fixed source schema. Real systems need additive-column-tolerant ingestion.
- **MLflow-tracked fraud model** scoring against `gold_customer_risk_features` and writing predictions back to a serving table.
- **Real cloud deployment** — currently Docker Compose only. Production would target Spark on Kubernetes (or EMR/Databricks) with managed Airflow (MWAA / Cloud Composer) and a real warehouse (Snowflake / BigQuery).
- **Lineage metadata** — OpenLineage emission from Airflow + Spark to a Marquez or DataHub instance.
- **CDC ingestion** — Debezium → Kafka for upstream operational databases.

---

## Author

Kıvanç Filizci — [LinkedIn](https://linkedin.com/in/kivancfilizci) · [GitHub](https://github.com/kivancfk)

Built April–June 2026 as a portfolio project for data-engineering roles in payments and fintech.
