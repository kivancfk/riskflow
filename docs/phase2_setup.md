# Phase 2 — Apply & Setup

This zip contains a filesystem-based Great Expectations workflow plus
an updated silver layer that renames `isFraud` → `is_fraud` and
`isFlaggedFraud` → `is_flagged_fraud` (silver naming convention).

## Prereqs

- Phase 1 closed and merged to `main`
- You're on a fresh `phase-2-silver` branch:
  ```bash
  git checkout main && git pull
  git checkout -b phase-2-silver
  ```
- Docker stack up: `docker compose up -d`

## Apply

```bash
unzip ~/Downloads/riskflow_phase2.zip -d /tmp/phase2
cp -r /tmp/phase2/* ~/Desktop/riskflow/

# Append the GE Make targets to your existing Makefile:
cat /tmp/phase2/Makefile.phase2 >> ~/Desktop/riskflow/Makefile
rm /tmp/phase2/Makefile.phase2  # cleanup, the targets are now in Makefile

git status
```

You should see:
```
new file:    great_expectations/great_expectations.yml
new file:    great_expectations/.gitignore
new file:    great_expectations/expectations/bronze_transactions_suite.json
new file:    great_expectations/expectations/silver_transactions_suite.json
new file:    great_expectations/checkpoints/bronze_checkpoint.yml
new file:    great_expectations/checkpoints/silver_checkpoint.yml
modified:    Makefile
modified:    airflow/dags/riskflow_daily_ingest.py
modified:    spark/jobs/silver_transform.py
modified:    tests/unit/test_silver_transform.py
modified:    tests/integration/test_silver_transform.py
```

## Pin Great Expectations version

Add this line to `pyproject.toml` (or your existing requirements file)
and rebuild the Airflow image:

```toml
"great-expectations==0.18.21",
```

Then:

```bash
docker compose build airflow
docker compose up -d
```

This pins us to the last stable 0.18 release. GX 1.x has breaking API
changes that would need significant rewrites; 0.18.21 is the right
target for this project's lifetime.

## Bootstrap

Once, after the first build:

```bash
make ge-init
```

This scaffolds the `uncommitted/` directory inside the GE project and
verifies the context loads cleanly. Expected output:

```
GE context OK — datasources: ['bronze_pandas', 'silver_pandas']
Suites: ['bronze_transactions_suite', 'silver_transactions_suite']
Checkpoints: ['bronze_checkpoint', 'silver_checkpoint']
```

If you see all three lines populated, the GE project is healthy.

## Test the pipeline

```bash
# Trigger a single day end-to-end
make run-day DAY=07
```

The DAG now has 6 tasks. Expected sequence:

1. `check_source_file_exists` → green (~5s)
2. `ingest_to_bronze` → green (~30-60s)
3. `validate_bronze` → **runs GE checkpoint** on bronze, green if 3 expectations pass
4. `transform_to_silver` → green (~30-60s)
5. `validate_silver` → **runs GE checkpoint** on silver, green if 3 expectations pass
6. `record_pipeline_run` → green

If any GE checkpoint fails, the corresponding task goes red, downstream
tasks are skipped, and `record_pipeline_run` records `status='failed'`.

## Verify GE artifacts

```bash
# What's in the GE project?
make ge-list

# Manually validate any day
make ge-validate-bronze DAY=07
make ge-validate-silver DAY=07

# Build HTML Data Docs
make ge-docs
open ./great_expectations/uncommitted/data_docs/local_site/index.html
```

The Data Docs page is the portfolio artifact — it shows every expectation,
every validation result, every run, in browsable HTML. Screenshot-friendly
for the README.

## Run unit tests

```bash
make test-unit
```

Should pass 30+ tests (expanded from Phase 1's 29).

## Backfill all 30 days

After day_07 runs cleanly:

```bash
for day in 01 02 03 04 05 06 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
  echo "▶ day_$day..."
  make run-day DAY=$day
  sleep 50  # bronze + GE + silver + GE takes ~50s total
done
```

~25 minutes for the full backfill.

## Phase 2 exit criteria

- [ ] `make ge-init` returns "GE context OK" with 2 datasources, 2 suites, 2 checkpoints
- [ ] DAG runs 6 tasks green for day_07
- [ ] All 30 silver partitions exist in `data/silver/`
- [ ] `pipeline_runs` has `silver_row_count` populated for all successful days
- [ ] `make ge-docs` produces browsable Data Docs HTML
- [ ] Pytest passes
- [ ] Both checkpoints fail loudly if you intentionally corrupt a partition

## Troubleshooting

**`make ge-init` reports "no datasources":** Your `great_expectations.yml`
didn't get copied. Re-run `cp -r /tmp/phase2/great_expectations/* ~/Desktop/riskflow/great_expectations/`.

**`validate_bronze` task fails with `CheckpointNotFoundError`:** The
checkpoint YAML isn't in the GE checkpoint store. Verify
`great_expectations/checkpoints/bronze_checkpoint.yml` exists.

**Silver task fails with `KeyError: 'isFraud'`:** You're still using
the Phase 1 silver_transform.py. Verify the new file landed via
`grep "is_fraud" spark/jobs/silver_transform.py`.

**GE Data Docs appear empty:** No checkpoints have been run yet. Run
`make ge-validate-bronze DAY=07` then `make ge-docs`.
