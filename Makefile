# RiskFlow — canonical entry points
# All operational commands flow through this Makefile so the README's
# "How to run locally" section stays accurate as the project evolves.

.PHONY: help up down init logs ps clean \
        split-data run-day run-streaming \
        test test-unit test-spark-integration \
        dbt-run dbt-test dbt-docs \
        ge-validate \
        lint format

# Default target: show available commands
help:
	@echo "RiskFlow — local development commands"
	@echo ""
	@echo "Lifecycle:"
	@echo "  make up                 Bring up the full stack (postgres, airflow, spark, kafka)"
	@echo "  make down               Stop the stack (preserves volumes)"
	@echo "  make clean              Stop and remove all volumes (destructive)"
	@echo "  make init               One-time setup: Airflow DB migrate + admin user"
	@echo "  make logs               Tail Airflow logs"
	@echo "  make ps                 Show service status"
	@echo ""
	@echo "Data:"
	@echo "  make split-data         Split data/raw/paysim.csv into 30 daily partitions"
	@echo "  make run-day DAY=01     Trigger riskflow_daily_ingest for day_01.csv"
	@echo "  make run-streaming      Start Kafka producer + Structured Streaming consumer"
	@echo ""
	@echo "Quality:"
	@echo "  make test               Pytest + dbt test"
	@echo "  make test-unit          Pytest unit tests only"
	@echo "  make test-spark-integration   Pytest integration tests only"
	@echo "  make dbt-run            Run all dbt models"
	@echo "  make dbt-test           Run all dbt tests"
	@echo "  make dbt-docs           Generate and serve dbt docs"
	@echo "  make ge-validate        Run Great Expectations checkpoints"
	@echo "  make lint               Run pre-commit hooks across all files"
	@echo "  make format             Auto-format Python with ruff"

# ---------- Lifecycle ----------

up:
	docker compose up -d
	@echo ""
	@echo "Stack is starting. UIs will be available at:"
	@echo "  Airflow:    http://localhost:8080  (admin / admin)"
	@echo "  Spark:      http://localhost:8081"
	@echo "  Kafka UI:   http://localhost:8090"

down:
	docker compose down

clean:
	docker compose down -v
	@echo "Stack stopped. Volumes removed."

init:
	docker compose up airflow-init

logs:
	docker compose logs -f airflow

ps:
	docker compose ps

# ---------- Data ----------

split-data:
	docker compose run --rm airflow \
	  python /opt/airflow/scripts/split_paysim.py \
	    --input  /opt/airflow/data/raw/paysim.csv \
	    --output /opt/airflow/data/partitioned

DAY ?= 01
run-day:
	@echo "Triggering riskflow_daily_ingest for day_$(DAY)"
	docker compose exec airflow \
	  airflow dags trigger riskflow_daily_ingest \
	    --conf '{"day": "$(DAY)"}'

run-streaming:
	docker compose exec airflow \
	  airflow dags trigger riskflow_streaming_consume

# ---------- Quality ----------

test-spark-integration:
	docker compose exec airflow pytest tests/integration -v

dbt-run:
	docker compose exec airflow \
	  bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."

dbt-test:
	docker compose exec airflow \
	  bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

dbt-docs:
	docker compose exec airflow \
	  bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8082"

ge-validate:
	docker compose exec airflow \
	  bash -c "cd /opt/airflow/great_expectations && great_expectations checkpoint run silver_checkpoint"

lint:
	pre-commit run --all-files

format:
	ruff format .
# ============================================================
# Phase 2 — Great Expectations targets
# Append these to your existing Makefile.
# ============================================================

.PHONY: ge-init ge-validate-bronze ge-validate-silver ge-docs ge-list

# One-shot bootstrap: scaffold uncommitted/ + verify the GE config loads.
# Idempotent — safe to re-run.
ge-init:
	@echo "▶ Bootstrapping GE project + verifying context loads..."
	docker compose exec airflow great_expectations \
		--config /opt/airflow/great_expectations \
		--no-usage-stats \
		project check-config || true
	docker compose exec airflow python -c \
		"import great_expectations as gx; \
		ctx = gx.get_context(context_root_dir='/opt/airflow/great_expectations'); \
		print('GE context OK — datasources:', list(ctx.list_datasources())); \
		print('Suites:', [s for s in ctx.list_expectation_suite_names()]); \
		print('Checkpoints:', ctx.list_checkpoints())"

# Run bronze checkpoint manually for a specific day
# Usage: make ge-validate-bronze DAY=07
ge-validate-bronze:
	@echo "▶ Validating bronze for day_$(DAY)..."
	docker compose exec airflow python -c \
		"import great_expectations as gx; \
		from pyspark.sql import SparkSession; \
		spark = SparkSession.builder.appName('ge_check').config('spark.ui.enabled','false').getOrCreate(); \
		df = spark.read.parquet('/opt/airflow/data/bronze/load_date=2026-04-$(DAY)').toPandas(); \
		spark.stop(); \
		ctx = gx.get_context(context_root_dir='/opt/airflow/great_expectations'); \
		r = ctx.run_checkpoint(checkpoint_name='bronze_checkpoint', batch_request={'datasource_name':'bronze_pandas','data_asset_name':'bronze_partition','runtime_parameters':{'batch_data':df},'batch_identifiers':{'load_date':'2026-04-$(DAY)'}}); \
		print('SUCCESS' if r['success'] else 'FAILED'); \
		print('Stats:', r.get('run_results'))"

# Run silver checkpoint manually for a specific day
# Usage: make ge-validate-silver DAY=07
ge-validate-silver:
	@echo "▶ Validating silver for day_$(DAY)..."
	docker compose exec airflow python -c \
		"import great_expectations as gx; \
		from pyspark.sql import SparkSession; \
		spark = SparkSession.builder.appName('ge_check').config('spark.ui.enabled','false').getOrCreate(); \
		df = spark.read.parquet('/opt/airflow/data/silver/load_date=2026-04-$(DAY)').toPandas(); \
		spark.stop(); \
		ctx = gx.get_context(context_root_dir='/opt/airflow/great_expectations'); \
		r = ctx.run_checkpoint(checkpoint_name='silver_checkpoint', batch_request={'datasource_name':'silver_pandas','data_asset_name':'silver_partition','runtime_parameters':{'batch_data':df},'batch_identifiers':{'load_date':'2026-04-$(DAY)'}}); \
		print('SUCCESS' if r['success'] else 'FAILED')"

# Build Data Docs locally
ge-docs:
	@echo "▶ Building GE Data Docs..."
	docker compose exec airflow python -c \
		"import great_expectations as gx; \
		ctx = gx.get_context(context_root_dir='/opt/airflow/great_expectations'); \
		ctx.build_data_docs()"
	@echo "▶ Data Docs built. Open in browser:"
	@echo "  open ./great_expectations/uncommitted/data_docs/local_site/index.html"

# Quick reference: what's in the GE project right now
ge-list:
	docker compose exec airflow python -c \
		"import great_expectations as gx; \
		ctx = gx.get_context(context_root_dir='/opt/airflow/great_expectations'); \
		print('Datasources:', [d['name'] for d in ctx.list_datasources()]); \
		print('Suites:', ctx.list_expectation_suite_names()); \
		print('Checkpoints:', ctx.list_checkpoints())"
# ============================================================
# Phase 3 — dbt + Postgres staging targets
#
# All target names are UNIQUE (no overlap with Phase 2's `dbt-test`
# and `dbt-docs`). Specifically:
#   - dbt-test-gold  (not dbt-test)
#   - dbt-docs-gold  (not dbt-docs)
#   - dbt-debug
#   - dbt-build-gold
#   - pg-init-staging
#   - pg-truncate-staging
#   - pg-row-counts
#
# Append these to your existing Makefile.
# ============================================================

.PHONY: dbt-debug dbt-build-gold dbt-test-gold dbt-docs-gold dbt-clean \
        pg-init-staging pg-truncate-staging pg-row-counts

# Run dbt's connection check
dbt-debug:
	docker compose exec airflow bash -c \
		"cd /opt/airflow/dbt && \
		DBT_PROFILES_DIR=/opt/airflow/dbt dbt debug --no-version-check"

# Build all gold models + their staging dependencies + run all tests.
# Same command Airflow runs in run_dbt_gold task.
dbt-build-gold:
	docker compose exec airflow bash -c \
		"cd /opt/airflow/dbt && \
		DBT_PROFILES_DIR=/opt/airflow/dbt \
		dbt build --select +gold --no-version-check"

# Run only tests (no rebuild) — Phase 3 specific
dbt-test-gold:
	docker compose exec airflow bash -c \
		"cd /opt/airflow/dbt && \
		DBT_PROFILES_DIR=/opt/airflow/dbt dbt test --select +gold --no-version-check"

# Generate dbt docs HTML
dbt-docs-gold:
	docker compose exec airflow bash -c \
		"cd /opt/airflow/dbt && \
		DBT_PROFILES_DIR=/opt/airflow/dbt && \
		dbt docs generate --no-version-check && \
		echo 'dbt docs at /opt/airflow/dbt/target/index.html'"

# Reset dbt artifacts (target/, dbt_packages/)
dbt-clean:
	docker compose exec airflow bash -c \
		"cd /opt/airflow/dbt && dbt clean"

# Apply the staging table DDL once on a fresh stack
pg-init-staging:
	docker compose exec -T postgres psql -U riskflow -d riskflow \
		< scripts/01_phase3_staging_table.sql

# Quick truncate (use only when you really want to wipe staging)
pg-truncate-staging:
	docker compose exec postgres psql -U riskflow -d riskflow \
		-c "TRUNCATE staging.silver_transactions;"

# Quick row-count sanity check across all schemas
pg-row-counts:
	docker compose exec postgres psql -U riskflow -d riskflow -c \
		"SELECT 'staging.silver_transactions' AS tbl, COUNT(*) FROM staging.silver_transactions \
		 UNION ALL \
		 SELECT 'dbt_staging.stg_silver_transactions',  COUNT(*) FROM dbt_staging.stg_silver_transactions \
		 UNION ALL \
		 SELECT 'dbt_staging.stg_pipeline_runs',        COUNT(*) FROM dbt_staging.stg_pipeline_runs \
		 UNION ALL \
		 SELECT 'gold.gold_daily_fraud_summary',          COUNT(*) FROM gold.gold_daily_fraud_summary \
		 UNION ALL \
		 SELECT 'gold.gold_customer_risk_features',       COUNT(*) FROM gold.gold_customer_risk_features \
		 UNION ALL \
		 SELECT 'gold.gold_transaction_velocity_features', COUNT(*) FROM gold.gold_transaction_velocity_features \
		 UNION ALL \
		 SELECT 'gold.gold_pipeline_quality_summary',     COUNT(*) FROM gold.gold_pipeline_quality_summary;"
# ============================================================
# Phase 4 — Kafka streaming targets
#
# Append to your existing Makefile.
# All target names are unique (no overlap with Phase 0/2/3 targets).
# ============================================================

.PHONY: streaming-init-topic streaming-up streaming-down \
        streaming-watch-db streaming-watch-log streaming-reset \
        test test-unit test-streaming-integration test-all

# Idempotent topic creation. Phase 4 owns this — Phase 0 only brought up
# the broker, not the topic. Re-running is a no-op (--if-not-exists).
streaming-init-topic:
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 \
		--create --if-not-exists \
		--topic riskflow.transactions \
		--partitions 3 --replication-factor 1
	@echo "✅ topic riskflow.transactions ready"

# Start producer + consumer.
# By default truncates streaming_alerts first (so demo runs are clean).
# Override with `KEEP=1 make streaming-up` to preserve previous alerts.
streaming-up: streaming-init-topic
ifndef KEEP
	@$(MAKE) streaming-reset
endif
	docker compose up -d streaming-producer streaming-consumer
	@echo ""
	@echo "✅ streaming pipeline started"
	@echo ""
	@echo "Watch in two terminals:"
	@echo "  pane 1: make streaming-watch-db"
	@echo "  pane 2: make streaming-watch-log"

streaming-down:
	docker compose stop streaming-producer streaming-consumer
	docker compose rm -f streaming-producer streaming-consumer

# Pane 1: live alert count + most-recent timestamp
streaming-watch-db:
	watch -n 1 'docker compose exec -T postgres psql -U riskflow -d riskflow -c \
		"SELECT count(*) AS alerts, max(flagged_at) AS latest \
		 FROM public.streaming_alerts;"'

# Pane 2: tail consumer logs
streaming-watch-log:
	docker compose logs -f --tail=50 streaming-consumer

# Truncate alerts. Used by streaming-up by default; can also be invoked
# manually between demo runs.
streaming-reset:
	docker compose exec -T postgres psql -U riskflow -d riskflow \
		-c "TRUNCATE public.streaming_alerts;"
	@echo "✅ streaming_alerts truncated"

# ============================================================
# Test split — fast unit tests by default, integration on demand.
# Integration tests are gated by pytest marker (see pyproject.toml).
# ============================================================

test-unit:
	docker compose exec -T airflow bash -c \
		"cd /opt/airflow && pytest -m 'not integration'"

test-streaming-integration:
	docker compose exec -T \
		-e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
		-e "PG_DSN=host=postgres port=5432 dbname=riskflow user=riskflow password=riskflow" \
		airflow bash -c \
		"cd /opt/airflow && pytest -m integration"
# Both — useful in CI
test-all:
	docker compose exec -T \
		-e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
		-e "PG_DSN=host=postgres port=5432 dbname=riskflow user=riskflow password=riskflow" \
		airflow bash -c \
		"cd /opt/airflow && pytest -m ''"
# Convenience alias — `make test` does the same thing as `make test-all`.
test: test-all
# ============================================================
# Phase 5 — Performance benchmarking targets
#
# Append to your existing Makefile.
# All target names are unique (no overlap with Phase 0/2/3/4 targets).
# ============================================================
#
# Per docs/performance.md §9, the harness runs on the host (psutil
# access, OS-level timing). The PySpark workload runs inside the
# existing Docker stack via `docker compose exec`. Pandas runs on the
# host directly. Pass 0 (this file) introduces:
#
#   perf-baseline   — pandas vs PySpark at small/medium/large scales
#   perf-all        — every Phase 5 benchmark (today: just perf-baseline)
#
# Variables (overridable from CLI):
#   PERF_RUNS=3          measured runs per (scale, implementation)
#   PERF_WARMUP=1        discarded warmup runs per (scale, implementation)
#   PERF_SCALES=small,medium,large
#   PERF_IMPLS=pandas,pyspark
#
# Example debugging incantation (fast):
#   make perf-baseline PERF_RUNS=1 PERF_WARMUP=0 PERF_SCALES=small
#
# Days 3-5 will add perf-partitioning, perf-broadcast, perf-skew.
# ============================================================

.PHONY: perf-baseline perf-all

PERF_RUNS    ?= 3
PERF_WARMUP  ?= 1
PERF_SCALES  ?= small,medium,large
PERF_IMPLS   ?= pandas,pyspark

perf-baseline:
	@echo "▶ Pass 0 — pandas vs PySpark baseline"
	@echo "   runs=$(PERF_RUNS) warmup=$(PERF_WARMUP) scales=$(PERF_SCALES) impls=$(PERF_IMPLS)"
	python scripts/perf_harness.py baseline \
	  --runs       $(PERF_RUNS) \
	  --warmup     $(PERF_WARMUP) \
	  --scales     $(PERF_SCALES) \
	  --implementations $(PERF_IMPLS)

# perf-all is the umbrella target. On Day 2 it's a thin wrapper around
# perf-baseline; Days 3-5 will add perf-partitioning, perf-broadcast,
# and perf-skew as additional prerequisites.
perf-all: perf-baseline
	@echo "✅ Phase 5 perf-all complete (Day 2: perf-baseline only)"
