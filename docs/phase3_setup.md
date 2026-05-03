# Phase 3 — Apply & Setup (v2, clean rewrite)

This zip adds the dbt gold-marts layer on top of Phase 2.

## What's new vs Phase 3 v1

- **Schema collision fixed.** Source data lives in `staging.silver_transactions`. dbt staging views land in `dbt_staging.*`. dbt gold tables land in `gold.*`. No more dbt cycle errors.
- **Source names prefixed `raw_`** (`raw_staging`, `raw_public`) to clearly separate sources from dbt schemas.
- **`generate_schema_name` macro** overrides dbt's default schema-prefixing so models go to the literal `+schema` value.
- **dbt 1.11 syntax** — uses `data_tests:` keyword and `arguments:` nesting for `accepted_values`. No more deprecation warnings.
- **Git installed in Airflow image** — fixes `dbt debug` "git [ERROR]" message.
- **Unique Makefile targets** — `dbt-test-gold` and `dbt-docs-gold` (no overlap with Phase 2's `dbt-test` and `dbt-docs`).

## Schema layout

| Postgres schema | Contents | Writer |
|---|---|---|
| `staging` | `silver_transactions` raw table | Spark JDBC |
| `dbt_staging` | `stg_silver_transactions`, `stg_pipeline_runs` views | dbt |
| `gold` | 4 gold mart tables | dbt |
| `public` | `pipeline_runs` operational table | Airflow `record_pipeline_run` |

## Prereqs

- Phase 2 merged to `main`
- New branch `phase-3-dbt-gold` checked out from current `main`
- Docker stack up

## Apply

```bash
cd ~/Desktop/riskflow
git checkout main && git pull
git checkout -b phase-3-dbt-gold

unzip ~/Downloads/riskflow_phase3.zip -d /tmp/phase3
cp -r /tmp/phase3/airflow /tmp/phase3/spark /tmp/phase3/dbt /tmp/phase3/scripts /tmp/phase3/tests /tmp/phase3/docs ./

# Append the Phase 3 Make targets to your existing Makefile
cat /tmp/phase3/Makefile.phase3 >> Makefile

git status
```

You should see modified `airflow/Dockerfile` (git added) and new files for the dbt project, scripts, and Spark JDBC writer.

## Rebuild Airflow image (git is now in apt-get install)

```bash
docker compose build airflow
docker compose up -d
```

Verify git is available:

```bash
docker compose exec airflow which git
# Expected: /usr/bin/git
```

## Bootstrap the staging tables and schemas

```bash
make pg-init-staging
```

This creates three schemas (`staging`, `dbt_staging`, `gold`) and the `staging.silver_transactions` table. Idempotent — safe to re-run.

Verify all three schemas exist:

```bash
docker compose exec postgres psql -U riskflow -d riskflow -c "\dn"
```

You should see `staging`, `dbt_staging`, `gold` in the schema list.

## Verify dbt connection

```bash
make dbt-debug
```

Expected output:
- `git [OK found]`
- `Connection test: [OK connection ok]`
- `All checks passed!`

## Smoke test on day_07

```bash
make run-day DAY=07
```

The DAG runs 8 tasks. Expected new task behavior:
- `load_silver_to_postgres` (~30-90s first run, ~30-60s subsequent) — Spark downloads JDBC driver from Maven, deletes day_07 from staging, writes 420k rows
- `run_dbt_gold` (~60-120s first run) — builds 2 staging views + 4 gold tables + runs 30+ tests

After the run:

```bash
make pg-row-counts
```

Expected for day_07 alone:

| Table | Rows |
|---|---|
| `staging.silver_transactions` | 420,583 |
| `dbt_staging.stg_silver_transactions` | 420,583 (it's a view) |
| `dbt_staging.stg_pipeline_runs` | however many DAG runs you've had |
| `gold.gold_daily_fraud_summary` | 5 (one per type) |
| `gold.gold_customer_risk_features` | ~530,000 |
| `gold.gold_transaction_velocity_features` | varies, customer-hour combos |
| `gold.gold_pipeline_quality_summary` | 1 (one partition_date so far) |

## Backfill all 30 days

```bash
for day in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
  echo "▶ day_$day..."
  make run-day DAY=$day
  sleep 90
done
```

~45 minutes for the full backfill.

After backfill:

```bash
make pg-row-counts
```

Expected:
- `staging.silver_transactions`: ~6.36M rows
- `gold.gold_daily_fraud_summary`: ~150 rows (30 days × ~5 types)
- `gold.gold_customer_risk_features`: ~6M rows
- `gold.gold_pipeline_quality_summary`: 30 rows

## Run dbt tests independently

```bash
make dbt-test-gold
```

All tests should pass. If any fail, the singular SQL tests in `dbt/tests/singular/` will print the offending rows for inspection.

## Generate dbt docs

```bash
make dbt-docs-gold
docker cp $(docker compose ps -q airflow):/opt/airflow/dbt/target/index.html ./dbt_docs.html
open ./dbt_docs.html
```

## Phase 3 exit criteria

- [ ] `make pg-init-staging` creates all three schemas
- [ ] `make dbt-debug` returns "Connection test: OK" with no git error
- [ ] `make run-day DAY=07` completes 8 green tasks
- [ ] After 30-day backfill, `make pg-row-counts` shows expected magnitudes
- [ ] `make dbt-test-gold` passes all tests
- [ ] `make dbt-docs-gold` produces browsable HTML
- [ ] `pytest tests/` passes (no regression on Phase 1/2 tests)

## Troubleshooting

**`make dbt-debug` shows "schema does not exist":**
You skipped `make pg-init-staging`. Run it.

**`load_silver_to_postgres` fails with "relation does not exist":**
The staging table wasn't created. Check `make pg-init-staging` output.

**`run_dbt_gold` fails with "Found a cycle":**
This was the v1 bug — should not happen in v2. If it does, verify
`dbt/dbt_project.yml` has `+schema: dbt_staging` (not `+schema: staging`)
and `dbt/macros/generate_schema_name.sql` exists.

**Spark fails with "ClassNotFoundException: org.postgresql.Driver":**
The `--packages` flag in the DAG should pull the JDBC driver
automatically on first run. Verify Spark container has internet access.
