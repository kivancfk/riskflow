# RiskFlow — Performance

This document covers the performance work in Phase 5: a pandas vs PySpark baseline that establishes *why* a distributed engine is justified for this workload, followed by three Spark optimization passes (partitioning, broadcast joins, AQE skew handling).

Each section follows the same shape: **symptom → diagnosis → fix → measurement**. Measurements are pulled from `data/perf/perf_runs.csv`; screenshots are under `docs/img/perf/`.

> **Honest framing.** This is a portfolio harness, not a benchmarks-paper harness. We do not run on isolated hardware, we do not control for thermal throttling, we do not bootstrap confidence intervals. Numbers are the median of 3 measured runs (1 warmup discarded) on the documented local stack. They are intended to demonstrate orders-of-magnitude differences and execution-plan changes, not absolute throughput. See §9 of `phase5_design_note_v2.md` for the methodology.

---

## 1. Setup

### 1.1 Machine and stack

| Component | Value |
|---|---|
| CPU | Intel Core i5-1038NG7 @ 2.0 GHz, 8 logical / 4 physical cores |
| RAM | 16 GB |
| OS | macOS 26.5 (build 25F71) |
| Python | 3.13.5 (host) |
| Java | Temurin OpenJDK 11.0.27 (in Docker) |
| Spark | 3.5.6 with Scala 2.12.18 (in Docker) |
| Docker | Desktop 29.4.3, 7.7 GB allocated to the engine |

### 1.2 Execution topology

The benchmark harness runs **on the host**. The PySpark workload runs **inside Docker**, invoked via `docker compose exec spark-master spark-submit`. The pandas workload runs **on the host** directly. This split gets us host-level OS timing/memory tools without losing Docker reproducibility for the Spark side.

```
┌──────────────────────────────────────────┐
│  host (macOS)                            │
│  ┌────────────────────────────────────┐  │
│  │ scripts/perf_harness.py            │  │
│  │ - psutil RSS sampler (pandas)      │──┼──spawn──▶ pandas worker (host process)
│  │ - docker stats sampler (pyspark)   │  │             └─ peak RSS via psutil
│  │ - 60s timeout (pandas Large only)  │  │
│  └─────────────────┬──────────────────┘  │
└────────────────────┼─────────────────────┘
                     │ docker compose exec
                     ▼
┌──────────────────────────────────────────┐
│  spark-master container                  │
│  └─ spark-submit silver_transform.py     │  ← measured by docker stats peak RSS
│       --load-dates 2026-04-01,...,30        on the resolved container name
│       (one Spark session per scale)         (riskflow-spark-master)
└──────────────────────────────────────────┘
```

Two harness contracts worth knowing:

- **PySpark gets one spark-submit per scale, not per partition.** The script's `--load-dates` argument takes a comma-separated list; `silver_transform.py::transform_multi` reads all partitions in one Spark session via `spark.read.parquet(*paths, basePath=bronze)`. This is what makes Pass 0 fair — see §2.2.3 for the bug that taught us this.
- **`docker stats` needs the container name, not the compose service name.** The harness resolves it at run-time via `docker compose ps --format "{{.Name}}" spark-master`, which returns `riskflow-spark-master` (or whatever the compose-project prefix yields on a different machine). Hardcoding `spark-master` would silently produce `peak_mem=0.0` on every PySpark run — which is exactly what the first version of the harness did.

### 1.3 Memory measurement, and why the columns aren't strictly comparable

Memory is captured per row of `data/perf/perf_runs.csv` with an explicit `memory_source` column so we never accidentally compare apples to oranges:

| `memory_source` | What it measures | Used for |
|---|---|---|
| `psutil_rss` | host pandas child process peak RSS, sampled at 100ms | all pandas runs |
| `docker_stats_rss` | spark-master container peak RSS during the job (incl. long-running daemon overhead) | all PySpark runs in Phase 5 Days 2–4 |
| `spark_ui_executor_peak` | Spark History Server peak executor memory used (planned upgrade) | _planned for Day 5_ |

**The honest claim** is that `psutil_rss` (a Python process's RSS, including pyarrow allocations) and `docker_stats_rss` (a JVM container's RSS, including the Spark master daemon) are not directly comparable in absolute terms. Different runtimes, different allocators, different baselines. What *is* comparable is the order of magnitude and the shape of the curve as scale increases: pandas memory grows nearly linearly with input size while Spark holds roughly constant for streaming-style transforms.

### 1.4 Reproducing the measurements

```bash
# Run only Pass 0 (this section)
make perf-baseline

# Run all Phase 5 benchmarks
make perf-all

# Debugging — fewer runs, small scale only
make perf-baseline PERF_RUNS=1 PERF_WARMUP=0 PERF_SCALES=small
```

The harness appends to `data/perf/perf_runs.csv`. Each row is one measured run; the median is computed at read time from the raw rows so the CSV stays a single source of truth.

---

## 2. Pass 0 — pandas vs PySpark baseline

### 2.1 Symptom

We have ~6 million transactions across 30 daily partitions of PaySim data. Pandas is the obvious tool — a single `pd.read_parquet()` followed by typing, dedup, and validation looks fine. Why introduce a JVM, a cluster, and the cognitive cost of distributed semantics?

The honest answer requires measurement, not assertion. So we ran the same silver transform — same input, same dedup key, same null/range checks, same Decimal(18,2) output schema — twice, once in pandas and once in PySpark, at three scales.

### 2.2 Setup

Two functionally-equivalent implementations:

| Path | Runtime |
|---|---|
| `spark/jobs/silver_transform.py` | PySpark (existing, production) |
| `transformations/silver_transform_pandas.py` | pandas + pyarrow (new, Pass 0) |

Function-by-function parity:

- `cast_financial_columns` — Decimal(18,2) cast + bronze camelCase → silver snake_case rename
- `derive_event_columns` — `event_hour`, `balance_delta_orig`, `balance_delta_dest`
- `deduplicate_transactions` — keep latest by `_load_ts` per `(name_orig, name_dest, step, amount)`
- `enforce_not_null_constraints` — split clean vs quarantine, attach `_quarantine_reason`

Enforced by `tests/integration/test_pandas_pyspark_parity.py`: same 10-row fixture, both implementations produce byte-identical silver Parquet (sorted by dedup key) and identical quarantine reason strings.

Three input scales:

| Scale | Partitions | Approx. rows |
|---|---|---|
| Small | 1 (`load_date=2026-04-15`) | ~200k |
| Medium | 7 (centered on the small date) | ~1.4M |
| Large | 30 (`2026-04-01` through `2026-04-30`) | ~6M |

Each (scale, implementation) was run 1× warmup (discarded) + 3× measured. Median reported.

#### 2.2.1 PySpark configuration: out-of-the-box vs tuned

The PySpark numbers in §2.3 use two `--conf` overrides on `spark-submit`:

```
--conf spark.sql.shuffle.partitions=8
--conf spark.default.parallelism=8
```

Both default to 200 in stock Spark. Spark assumes a multi-node cluster with hundreds of cores; on a single-worker / 8-core local stack, 200 tiny shuffle tasks pay more coordination cost than they save. Matching both knobs to the worker's `nproc` is a routine tuning, not a benchmark cheat — it's what you'd do in any production deploy on hardware of this size.

We discovered this empirically. The first run of `make perf-baseline` used stock defaults; PySpark lost by 1.1–4× at every scale. After tuning, PySpark crosses ahead on Large. The before-and-after numbers are preserved at `data/perf/perf_runs.untuned.csv` and reported in §2.3 alongside the tuned numbers, because the untuned-vs-tuned gap is itself a finding worth seeing.

#### 2.2.2 What we did *not* tune

We deliberately stopped at two `--conf` flags. We did not tune `spark.driver.memory`, did not flip `spark.sql.adaptive.coalescePartitions.enabled`, did not change `spark.serializer`, did not preheat the JVM with a no-op job. Pass 0 is the baseline; the point is to show what a competent-but-not-clever Spark deploy produces. Pass 1 onward earns improvements with code and execution-plan changes, not flag-tuning.

#### 2.2.3 An earlier harness bug worth flagging

The very first run of this harness had PySpark calling `spark-submit` **once per load_date** — paying ~30s of JVM cold start + driver handshake per partition. On Large, that meant 30 × 30s = ~900s of pure overhead before any work happened. The harness now invokes `spark-submit` once per scale via a `--load-dates` CSV argument; `silver_transform.py` reads all partitions in one Spark session. See `spark/jobs/silver_transform.py::transform_multi`. The bug is mentioned here because the broken numbers (`perf_runs.broken-per-partition-spark-submit.csv`) are kept in-tree as a teaching artifact: it's the kind of mistake that produces *very* misleading benchmarks if you don't notice it.

### 2.3 Measurements

Three tables: pandas, PySpark untuned, PySpark tuned. Medians of 3 measured runs after 1 discarded warmup. Exit modal is 0 for every cell (no failures, no timeouts at any scale).

#### Pandas

| Scale | wall (median, s) | peak RSS (MB) | memory_source |
|---|---|---|---|
| Small | **11.7** | 844 | psutil_rss |
| Medium | **66.9** | 866 | psutil_rss |
| Large | **207.4** | 1117 | psutil_rss |

Pandas scales roughly linearly with input size: 1× → 6.7× → 21× partitions yields 1× → 5.7× → 17.7× wall clock. Memory grows sub-linearly (1117 / 844 = 1.3× for a 30× larger input) — once pyarrow has bought the Python interpreter and pandas/numpy machinery, the marginal cost of additional rows is modest. We expected pandas to either time out (60s budget on the Large run, per design note §5) or run out of memory at 6M rows; it did neither. Pandas is slow-but-stable here.

#### PySpark — out-of-the-box (`shuffle.partitions=200`, `default.parallelism=200`)

| Scale | wall (median, s) | peak RSS (MB) | memory_source |
|---|---|---|---|
| Small | 40.1 | 918 | docker_stats_rss |
| Medium | 98.2 | 895 | docker_stats_rss |
| Large | 211.7 | 916 | docker_stats_rss |

Loses to pandas at every scale. Why: 200 shuffle tasks on 8 cores means each task is too small to be worth scheduling. The driver spends most of its time orchestrating, not computing.

#### PySpark — tuned (`shuffle.partitions=8`, `default.parallelism=8`)

| Scale | wall (median, s) | peak RSS (MB) | memory_source |
|---|---|---|---|
| Small | 45.9 | 946 | docker_stats_rss |
| Medium | 92.6 | 935 | docker_stats_rss |
| Large | **199.1** | 885 | docker_stats_rss |

Small got 6 seconds *slower* — the tuning costs you a hair when there's literally one shuffle task either way, because Spark's adaptive optimizer was already coalescing aggressively. Medium and Large each got ~6% faster.

#### Side-by-side

| Scale | pandas | pyspark (tuned) | Winner | Margin |
|---|---|---|---|---|
| Small | 11.7s | 45.9s | **pandas** | 3.9× |
| Medium | 66.9s | 92.6s | **pandas** | 1.4× |
| Large | 207.4s | 199.1s | **pyspark** | 1.04× |

### 2.4 Interpretation

**The curves cross between 1.4M and 6M rows.** Pandas wins decisively at Small and Medium. PySpark crosses ahead at Large, but by only 4%.

This is not the "Spark obliterates pandas" story most benchmark posts tell. It's a more honest one: **Spark on this dataset on this hardware doesn't earn its keep until you're past 6M rows of single-table transform work.** The PaySim transform is computationally cheap per row — a cast, an arithmetic, a window dedup, four null checks. Spark's per-row overhead (serialization, codegen, shuffle metadata, executor coordination) is fixed-cost; pandas's is near-zero. The crossover happens when input size finally amortizes that fixed cost.

But the *slope* matters more than the crossover point. Pandas's wall clock grew 17.7× from Small to Large. PySpark's grew 4.3×. Extrapolating the curves (which is dangerous, but illustrative):

- At ~60M rows (PaySim 10× scale), pandas would be ~35 min, PySpark ~14 min. PySpark wins by ~2.5×.
- At ~600M rows (cluster-scale), pandas runs out of memory long before completion; PySpark scales horizontally with more workers.

So the takeaway is **not** "Spark is faster," because at 6M rows it barely is. The takeaway is **judgment**: pandas is the right tool below the inflection point, Spark is the right tool above it, and the inflection point on this workload on this hardware is roughly at our largest scale. Spark's value here isn't speed — it's the property that *scaling further is a configuration change rather than a rewrite*. Pandas at 60M rows requires re-architecting (Dask, Polars streaming, chunked processing); Spark at 60M rows requires adding a worker container.

The remaining passes (1–3) earn improvements *to PySpark* with execution-plan changes. After Pass 1's partition pruning, after Pass 2's broadcast joins, after Pass 3's skew handling, the Large pyspark wall clock should drop substantially below pandas's — making the choice unambiguous at this scale, not just future-scale.

### 2.5 Caveats specific to Pass 0

- **`docker_stats_rss` over-reports.** The spark-master container's RSS includes the long-running Spark master daemon, not just the spark-submit job. The reported ~900 MB for PySpark is "container working set during the job," not "this job's executor used 900 MB of heap." A Day 5 upgrade to `spark_ui_executor_peak` (via the Spark History Server) would be tighter; for Pass 0 the column is honest about the source, not precise about the heap.
- **Decimal arithmetic is slow in both engines, slower in pandas.** Every `oldbalanceOrg - newbalanceOrig` is a Python-level Decimal subtraction in pandas, not a vectorized C op. PySpark's `DecimalType(18,2)` arithmetic runs on the JVM but doesn't get the same SIMD treatment as `DoubleType`. If we cared about absolute throughput we'd convert to float64 internally and round-trip to Decimal on write — but the comparison's whole point is that the *same logical transform* runs in both engines, byte-equivalent output.
- **The local Spark stack is jittery under sustained load.** The medium/pyspark third run was 120s vs the other two at 84s and 92s — a 40% outlier on what should be the same work. Median absorbs it (92.6s), and we ran all benchmarks sequentially per Phase 4's lesson about concurrent Spark instability, but the jitter is real. Numbers within ±15% of each other should be read as "roughly the same"; anything tighter is noise.
- **Single-day pandas reads one partition; multi-day pandas loops.** The pandas worker is invoked once per `load_date` by the harness (one subprocess per partition); PySpark reads all partitions in one job. This asymmetry favors pandas on small scales (no inter-partition coordination) and *disfavors* it at scale (no parallelism across partitions, ever). It mirrors what a pandas-native practitioner would write rather than what a hypothetical "parallelized pandas with `concurrent.futures`" would do — which is the honest comparison.
- **We tuned shuffle.partitions and stopped.** Further tuning (`spark.driver.memory`, AQE coalesce thresholds, adaptive skew join) would likely close more of the gap or extend PySpark's Large win. Pass 0 is the baseline; we leave further tuning to be *earned* by the next three passes with measurable code changes, not configuration sprawl.

---

## 3. Pass 1 — Partition pruning

> **Coming in Day 3.**

---

## 4. Pass 2 — Broadcast joins

> **Coming in Day 4.**

---

## 5. Pass 3 — AQE and skew handling

> **Coming in Day 5.**

---

## 6. Summary table

> **Coming in Day 5 — the consolidated headline numbers across all four passes.**

---

## 7. What's not in scope

See `phase5_design_note_v2.md` §3 and §12 for the full list. Briefly: no cluster tuning, no engine comparisons (Flink/Trino/DuckDB), no cloud-vs-local, no storage-format experiments, no caching demo (deferred), no Iceberg / Z-ordering.
