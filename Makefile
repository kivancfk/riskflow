# RiskFlow — canonical entry points
# All operational commands flow through this Makefile so the README's
# "How to run locally" section stays accurate as the project evolves.

.PHONY: help up down init logs ps clean \
        split-data run-day run-streaming \
        test test-unit test-integration \
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
	@echo "  make test-integration   Pytest integration tests only"
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

test: test-unit dbt-test

test-unit:
	docker compose exec airflow pytest tests/unit -v

test-integration:
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
