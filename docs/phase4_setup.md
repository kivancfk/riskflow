# Phase 4 — Apply & Setup

This adds the Kafka streaming layer (producer + consumer + `streaming_alerts`)
on top of Phase 3. Track A (kafka-python). The batch ELT pipeline is
unaffected.

## What's new vs Phase 3

| Item | Detail |
|---|---|
| `streaming/` directory | parallel to `airflow/`, `spark/`, `dbt/` |
| `public.streaming_alerts` table | one row per FLAG decision, idempotent inserts |
| Two new Docker services | `streaming-producer`, `streaming-consumer`, long-running, NOT Airflow-orchestrated |
| Kafka topic | `riskflow.transactions` (3 partitions, replication 1) |
| Velocity rule | pure-function `evaluate_velocity_rule(recent, thresholds)` |
| Test split | `test-unit` (fast, default) vs `test-streaming-integration` (gated) |

## Prereqs

- Phase 3 merged to `main`
- New branch `phase-4-kafka-streaming` checked out from current `main`
- Docker stack up
- A few `load_date=` partitions exist under `data/silver/` (run a few backfills first)

## Apply

```bash
cd ~/Desktop/riskflow
git checkout main && git pull
git checkout -b phase-4-kafka-streaming

unzip ~/Downloads/riskflow_phase4.zip -d /tmp/phase4
# (the zip extracts into /tmp/phase4/riskflow_phase4/...)

cp -r /tmp/phase4/riskflow_phase4/streaming \
      /tmp/phase4/riskflow_phase4/scripts ./
cp /tmp/phase4/riskflow_phase4/docs/phase4_setup.md ./docs/
cp /tmp/phase4/riskflow_phase4/docs/adr_006_phase4_streaming.md ./docs/
# (then paste the ADR-006 contents into docs/decisions.md per the patch file)

# Append docker-compose services
cat /tmp/phase4/riskflow_phase4/docker-compose.phase4.snippet.yml >> docker-compose.yml
# (then manually move the appended block ABOVE the `volumes:` section)

# Append Make targets
cat /tmp/phase4/riskflow_phase4/Makefile.phase4 >> Makefile

# Merge pyproject.toml additions (replace existing [tool.pytest.ini_options])
$EDITOR pyproject.toml
# paste contents of /tmp/phase4/riskflow_phase4/pyproject.phase4.snippet.toml

git status
```

You should see new `streaming/`, `scripts/02_streaming_alerts_table.sql`,
`docs/phase4_setup.md`, and modifications to `docker-compose.yml`,
`Makefile`, `pyproject.toml`.

## Bootstrap

### 0. PREFLIGHT — verify silver schema column names

Producer assumes silver Parquet has **snake_case** columns: `step`, `type`,
`amount`, `name_orig`, `name_dest`, `is_fraud`. PaySim's raw CSV uses
**camelCase** (`nameOrig`, `nameDest`, `isFraud`). If Phase 2's silver writer
didn't normalize, the producer will fail with `AttributeError: 'Pandas' object
has no attribute 'name_orig'`. Verify before running:

```bash
docker compose exec -T airflow python - <<'PY'
import pyarrow.dataset as ds
d = ds.dataset("/opt/airflow/data/silver", format="parquet")
print(d.schema)
PY
```

Expected output should include `name_orig`, `name_dest`, `is_fraud`. If
you see camelCase instead, you have two options:

- **Preferred:** fix Phase 2's silver writer to rename columns to snake_case.
  Silver should be normalized anyway — schema drift between layers is a
  Phase-2 hygiene issue, not a Phase 4 problem.
- **Quick fix:** add a column-mapping shim at the top of
  `streaming/producer.py`'s `iter_silver_rows` (rename `nameOrig` →
  `name_orig` etc. as the dataframe loads). Document the workaround as
  technical debt in the PR description.

### 1. Rebuild the Airflow image
The streaming services build from `airflow/Dockerfile` (which already pins
`kafka-python==2.0.2` from Phase 0), but new mounts mean a rebuild:

```bash
docker compose build airflow streaming-producer streaming-consumer
docker compose up -d
```

### 2. Create the `streaming_alerts` table

```bash
docker compose exec -T postgres psql -U riskflow -d riskflow \
  < scripts/02_streaming_alerts_table.sql
```

Verify:
```bash
docker compose exec postgres psql -U riskflow -d riskflow \
  -c "\d public.streaming_alerts"
```

You should see the `uq_streaming_alert_txn_rule` constraint on
`(transaction_id, rule_name)`.

### 3. Create the Kafka topic

```bash
make streaming-init-topic
```

Verify in Kafka UI at http://localhost:8090 — `riskflow.transactions`
should appear with 3 partitions.

## Smoke test (60-second demo)

```bash
make streaming-up
```

In a second terminal pane:
```bash
make streaming-watch-db
```

In a third terminal pane:
```bash
make streaming-watch-log
```

With `DEMO_BURST=true` (the default in the compose snippet), you should see
the alert count increment within ~60 seconds. The consumer log should show
`FLAG name_orig=C_DEMO_BURST ...` lines.

When done:
```bash
make streaming-down
```

## Run unit tests

```bash
make test-unit
```

Should run all 8 fraud_rule tests in <1 second, plus any earlier-phase units.

## Run integration tests

```bash
make test-streaming-integration
```

These spawn the consumer as a subprocess against the live Kafka + Postgres
containers. Slower (~60-90s total). Each test uses a unique customer name
so they don't collide.

## Post-hoc rule evaluation (precision/recall vs PaySim `isFraud`)

The producer publishes the upstream `is_fraud` label alongside each
transaction (it's not used in the rule itself, just passed through). After
running the pipeline for a while, you can join the alerts back to silver
to get a confusion matrix:

```sql
-- Confusion matrix of velocity rule vs PaySim ground truth
WITH alerted AS (
    SELECT DISTINCT transaction_id, name_orig
    FROM public.streaming_alerts
    WHERE rule_name = 'velocity_breach'
)
SELECT
    CASE WHEN a.transaction_id IS NOT NULL THEN 'flagged' ELSE 'not_flagged' END AS rule_decision,
    s.is_fraud,
    count(*) AS n
FROM staging.silver_transactions s
LEFT JOIN alerted a USING (transaction_id)
GROUP BY 1, 2
ORDER BY 1, 2;
```

Expected shape (numbers depend on how long the pipeline ran):

| rule_decision | is_fraud | n |
|---|---|---|
| flagged       | 0 | (false positives) |
| flagged       | 1 | (true positives) |
| not_flagged   | 0 | (true negatives) |
| not_flagged   | 1 | (false negatives) |

This is **not** the rule's primary purpose — it's a velocity heuristic, not
a classifier. But it's a free interview talking point about rule quality
and the gap between rule-based and ML-based fraud detection.

> Caveat: the `transaction_id` join only works if Phase 2's silver writer
> generates IDs with the same SHA-256 prefix scheme as the producer. For
> Phase 4 the producer is the only writer; aligning the silver writer is
> a Phase 5+ task and should be added to ADR-006's follow-up list.

## Phase 4 exit criteria

- [ ] DDL applied: `\d public.streaming_alerts` shows the unique constraint
- [ ] Topic created: `make streaming-init-topic` succeeds, visible in Kafka UI
- [ ] `make streaming-up` brings both services up cleanly
- [ ] With `DEMO_BURST=true`, an alert appears within 60 seconds
- [ ] `make test-unit` passes (8 new tests + previous-phase tests)
- [ ] `make test-streaming-integration` passes 3 tests
- [ ] Replaying the same messages does NOT double the alert count (idempotency)
- [ ] `docker compose restart streaming-consumer` — consumer comes back, resumes
      from committed offset; in-memory state is lost (acceptable, see ADR-006)
- [ ] README §3 / §5 updated; ADR-006 appended

## Troubleshooting

**Consumer logs `KafkaError: NoBrokersAvailable`.**
The Kafka container isn't ready yet. `docker compose ps kafka` should show
`healthy`/`running`; restart the consumer with `docker compose restart streaming-consumer`.

**Producer logs `FileNotFoundError: silver path not found`.**
You haven't backfilled silver yet. Run a few `make run-day DAY=NN` first
so `data/silver/load_date=*/` partitions exist.

**`streaming_alerts` row count doesn't grow.**
Three checks: (1) producer is publishing (`docker compose logs streaming-producer`
should show `cycle=N published=…` lines); (2) consumer is consuming (Kafka UI
shows non-zero offset for the consumer group); (3) `DEMO_BURST=true` is set
(natural PaySim ordering may not produce a cluster).

**Integration tests can't connect to Kafka.**
By default they use `localhost:29092` (the EXTERNAL listener). If you're
running them inside a container, set `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`.

**`make streaming-up` fails with `KEEP` warnings.**
That's not a real failure. Make is conditional-evaluating the `KEEP` var.
The actual error is above the warning — usually the topic doesn't exist
(re-run `make streaming-init-topic`) or the Postgres table doesn't exist
(re-run the DDL).
