"""RiskFlow performance benchmark harness (Phase 5).

Runs on the host (macOS or Linux dev box). Invokes the implementation
under test as a subprocess — pandas runs locally, PySpark runs inside
the existing Docker stack via `docker compose exec`. Captures wall
clock + peak memory, writes one row per measured run to
data/perf/perf_runs.csv, and prints the median per (scale,
implementation) at the end.

Design choices documented in docs/performance.md §9. The most
important ones, recapped:

  - Subprocess per run, not in-process. Clean memory measurement,
    isolation against pandas OOM, trivial timeout enforcement.
  - 1 warmup + 3 measured runs by default. Warmup is discarded to
    paper over Python/JVM cold-start cost.
  - Memory source is explicit per row in the CSV. pandas reports
    psutil-sampled child RSS; PySpark reports `docker stats` peak
    container RSS as a Day 2 proxy (Spark UI executor peak is the
    Day 5 upgrade).
  - 60-second hard timeout on the pandas Large run. Other scales and
    PySpark runs are unbounded. The timeout is per-run, not total.
  - Sequential execution only. The local Spark stack is unstable
    under concurrent load (lesson from Phase 4 / Day 1).

CSV schema:
    benchmark_name, scale, implementation, run_index, wall_clock_seconds,
    peak_memory_mb, memory_source, exit_status, notes, run_timestamp

Usage:
    python scripts/perf_harness.py baseline [--runs 3] [--warmup 1] \\
        [--scales small,medium,large] \\
        [--implementations pandas,pyspark] \\
        [--bronze-dir data/bronze] \\
        [--output-root data/perf/pass0]
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import shutil
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

# psutil is a host-side dependency. We import it lazily so a host
# without psutil can still see the help text and fail with a clear
# error only when it actually tries to measure something.
try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None  # type: ignore[assignment]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("perf_harness")


# ---------------------------------------------------------------
# Scale definitions
# ---------------------------------------------------------------
# Pin a base date in the middle of the 30-day window so `medium` (7
# days centered on the base) and `large` (full window) overlap cleanly
# with `small` (1 day). PaySim was split into load_dates 2026-04-01
# through 2026-04-30 by scripts/split_paysim.py per the Phase 0 setup.

PERF_DATE_START = date(2026, 4, 1)
PERF_DATE_END = date(2026, 4, 30)
BASE_DATE = date(2026, 4, 15)  # the day_15 partition


def _date_range(start: date, end: date) -> list[str]:
    """Inclusive date range as ISO YYYY-MM-DD strings."""
    out, cur = [], start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


SCALES: dict[str, list[str]] = {
    "small":  [BASE_DATE.isoformat()],
    "medium": _date_range(BASE_DATE - timedelta(days=3),
                          BASE_DATE + timedelta(days=3)),  # 7 days
    "large":  _date_range(PERF_DATE_START, PERF_DATE_END),  # 30 days
}


# ---------------------------------------------------------------
# CSV schema
# ---------------------------------------------------------------
CSV_COLUMNS = [
    "benchmark_name", "scale", "implementation", "run_index",
    "wall_clock_seconds", "peak_memory_mb", "memory_source",
    "exit_status", "notes", "run_timestamp",
]


# ---------------------------------------------------------------
# Run result
# ---------------------------------------------------------------
@dataclass
class RunResult:
    benchmark_name: str
    scale: str
    implementation: str
    run_index: int
    wall_clock_seconds: float
    peak_memory_mb: float
    memory_source: str
    exit_status: int
    notes: str
    run_timestamp: str

    def as_csv_row(self) -> dict[str, str]:
        return {
            "benchmark_name":      self.benchmark_name,
            "scale":               self.scale,
            "implementation":      self.implementation,
            "run_index":           str(self.run_index),
            "wall_clock_seconds":  f"{self.wall_clock_seconds:.3f}",
            "peak_memory_mb":      f"{self.peak_memory_mb:.1f}",
            "memory_source":       self.memory_source,
            "exit_status":         str(self.exit_status),
            "notes":               self.notes,
            "run_timestamp":       self.run_timestamp,
        }


# ---------------------------------------------------------------
# Memory sampler — psutil RSS at 100ms intervals
# ---------------------------------------------------------------
class _RSSSampler(threading.Thread):
    """Background thread sampling a child pid's RSS every 100ms.

    Stops on .stop(). Tolerates the child exiting under it (NoSuchProcess).
    Peak RSS in bytes is exposed via .peak_bytes.
    """

    INTERVAL_SECONDS = 0.1

    def __init__(self, pid: int) -> None:
        super().__init__(daemon=True)
        self._pid = pid
        self._stop_event = threading.Event()
        self.peak_bytes: int = 0

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        if psutil is None:
            return
        try:
            proc = psutil.Process(self._pid)
        except psutil.NoSuchProcess:
            return
        while not self._stop_event.is_set():
            try:
                # Include any subprocesses pandas may spawn (it doesn't,
                # but pyarrow's IO uses thread pools; rss already covers
                # threads since they share address space).
                rss = proc.memory_info().rss
                if rss > self.peak_bytes:
                    self.peak_bytes = rss
            except psutil.NoSuchProcess:
                return
            except psutil.AccessDenied:
                return
            self._stop_event.wait(self.INTERVAL_SECONDS)


# ---------------------------------------------------------------
# Workload runners
# ---------------------------------------------------------------
@dataclass
class RunSpec:
    """One subprocess execution unit. Builds the argv per load_date."""
    implementation: str  # "pandas" | "pyspark"
    load_dates: list[str]
    bronze_dir: Path
    output_root: Path
    timeout_seconds: float | None  # None = no timeout
    run_label: str  # for run-id / app-name


def _pandas_argv(spec: RunSpec, load_date: str) -> list[str]:
    """argv for one pandas subprocess (one load_date partition)."""
    silver_dir = spec.output_root / "pandas" / "silver"
    quarantine_dir = spec.output_root / "pandas" / "quarantine"
    return [
        sys.executable, "-m", "transformations.silver_transform_pandas",
        "--bronze-dir",     str(spec.bronze_dir),
        "--silver-dir",     str(silver_dir),
        "--quarantine-dir", str(quarantine_dir),
        "--load-date",      load_date,
        "--load-ts",        datetime.now(timezone.utc).isoformat(),
        "--run-id",         f"{spec.run_label}__{load_date}",
    ]


def _pyspark_argv(spec: RunSpec, load_dates: list[str]) -> list[str]:
    """argv for one PySpark subprocess processing N load_dates in one session.

    Phase 5 Day 2 fix: previously this function took a single load_date
    and produced one spark-submit per date. For N=30 the JVM cold-start
    cost was paid 30 times, dwarfing actual work. Now we pass all N
    dates via --load-dates (plural) and silver_transform.py reads them
    in one Spark session.

    Paths reflect what's actually inside the spark-master container:
      - spark-submit lives at /opt/spark/bin/spark-submit (not on PATH)
      - silver_transform.py lives at /opt/airflow/spark_jobs/... (the
        host-side spark/jobs/ is renamed by the docker-compose mount)
    """
    return [
        "docker", "compose", "exec", "-T", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        # Tune shuffle/parallelism to match the worker's available cores.
        # Default spark.sql.shuffle.partitions=200 is wildly over-provisioned
        # for a 1-worker / 8-core local stack — 200 tiny shuffle tasks pay
        # more coordination cost than they save. Setting both to 8 matches
        # the available parallelism. This is documented as Pass 0 "tuned"
        # config in docs/performance.md §2; Pass 0 "out-of-the-box" numbers
        # (without these flags) are preserved in
        # data/perf/perf_runs.untuned.csv for the before/after story.
        "--conf", "spark.sql.shuffle.partitions=8",
        "--conf", "spark.default.parallelism=8",
        "/opt/airflow/spark_jobs/silver_transform.py",
        "--bronze-dir",     "/opt/airflow/data/bronze",
        "--silver-dir",     f"/opt/airflow/{spec.output_root}/pyspark/silver",
        "--quarantine-dir", f"/opt/airflow/{spec.output_root}/pyspark/quarantine",
        "--load-dates",     ",".join(load_dates),
        "--load-ts",        datetime.now(timezone.utc).isoformat(),
        "--run-id",         f"{spec.run_label}__multi_{len(load_dates)}dates",
    ]


def _run_pandas_partition(
    spec: RunSpec, load_date: str,
) -> tuple[float, int, int, str]:
    """Run one pandas subprocess. Returns (peak_bytes, exit_code, wall_ns_approx, notes).

    wall_ns_approx is the per-partition portion of wall clock; the
    caller sums across partitions.
    """
    argv = _pandas_argv(spec, load_date)
    notes = ""
    try:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as e:
        return (0, 127, 0, f"spawn_failed:{e}")

    sampler = _RSSSampler(proc.pid)
    sampler.start()

    t0 = time.perf_counter_ns()
    try:
        proc.communicate(timeout=spec.timeout_seconds)
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        exit_code = 124  # conventional timeout exit code
        notes = f"timeout_{int(spec.timeout_seconds)}s"
    finally:
        sampler.stop()
        sampler.join(timeout=1.0)
    t1 = time.perf_counter_ns()

    return (sampler.peak_bytes, exit_code, t1 - t0, notes)


def _run_pyspark_all(
    spec: RunSpec,
) -> tuple[float, int, int, str]:
    """Run ALL load_dates in a single PySpark session via one spark-submit.

    Returns (peak_bytes, exit_code, wall_ns, notes).

    Phase 5 Day 2 fix: was previously called per-load-date in a loop,
    paying ~30s JVM cold start on every call. Now we hand the
    underlying script a comma-separated list of dates and let the
    Spark driver iterate.
    """
    argv = _pyspark_argv(spec, spec.load_dates)
    notes = ""
    #container = "spark-master"
    container = _resolve_container_name("spark-master")
    if container is None:
        log.warning("Could not resolve container name for spark-master; "
                    "peak memory will be 0.")
    try:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as e:
        return (0, 127, 0, f"spawn_failed:{e}")
    sampler = _DockerStatsSampler(container=container or "spark-master")
    #sampler = _DockerStatsSampler(container=container)
    sampler.start()

    t0 = time.perf_counter_ns()
    try:
        proc.communicate(timeout=spec.timeout_seconds)
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        exit_code = 124
        notes = f"timeout_{int(spec.timeout_seconds)}s"
    finally:
        sampler.stop()
        sampler.join(timeout=3.0)
    t1 = time.perf_counter_ns()

    return (sampler.peak_bytes, exit_code, t1 - t0, notes)


class _DockerStatsSampler(threading.Thread):
    """Background thread sampling a container's RSS via `docker stats`.

    Day 2 of Phase 5 had a poll-during-job loop inline in the PySpark
    runner; it produced peak=0.0MB for every measured run because
    `docker stats --no-stream` on macOS Docker Desktop has ~1-2s of
    latency, and the inline poll competed with `subprocess.communicate`
    for I/O attention. Moving this to a dedicated daemon thread (mirror
    of _RSSSampler) decouples sampling from the spark-submit lifecycle
    and gives every sample a fair chance to land.

    Sample interval is 1.5s — slightly longer than docker stats's own
    cost — so we don't queue up overlapping invocations on slow runs.
    """

    SAMPLE_INTERVAL_SECONDS = 1.5
    DOCKER_STATS_TIMEOUT_SECONDS = 5.0

    def __init__(self, container: str) -> None:
        super().__init__(daemon=True)
        self._container = container
        self._stop_event = threading.Event()
        self.peak_bytes: int = 0

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                out = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format",
                     "{{.MemUsage}}", self._container],
                    capture_output=True, text=True,
                    timeout=self.DOCKER_STATS_TIMEOUT_SECONDS,
                )
                if out.returncode == 0 and out.stdout.strip():
                    mem_str = out.stdout.strip().split("/")[0].strip()
                    bytes_used = _parse_docker_mem(mem_str)
                    if bytes_used > self.peak_bytes:
                        self.peak_bytes = bytes_used
            except (subprocess.TimeoutExpired, ValueError):
                pass
            # Wait on the stop event so we exit promptly on .stop()
            # rather than sleeping out the full interval.
            self._stop_event.wait(self.SAMPLE_INTERVAL_SECONDS)

def _resolve_container_name(compose_service: str) -> str | None:
    """Resolve a docker-compose service name to its actual container name.

    `docker compose exec` accepts the service name (e.g. `spark-master`),
    but `docker stats` needs the container name (e.g.
    `riskflow-spark-master`). The container name is the compose-project
    prefix + service + optional index, and depends on the directory
    name docker-compose was invoked from — so we can't hardcode it.

    Returns None if `docker compose ps` can't resolve it.
    """
    try:
        out = subprocess.run(
            ["docker", "compose", "ps", "--format", "{{.Name}}",
             compose_service],
            capture_output=True, text=True, timeout=5.0,
        )
        if out.returncode == 0 and out.stdout.strip():
            # Take the first non-empty line.
            for line in out.stdout.splitlines():
                line = line.strip()
                if line:
                    return line
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None

def _parse_docker_mem(s: str) -> int:
    """Parse '1.23GiB' / '512MiB' / '42KiB' / '512B' → bytes.

    docker stats uses IEC binary units. Returns 0 on parse failure.
    """
    s = s.strip()
    suffixes = [
        ("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024),
        ("GB", 1000**3), ("MB", 1000**2), ("KB", 1000),
        ("B", 1),
    ]
    for suffix, mult in suffixes:
        if s.endswith(suffix):
            try:
                return int(float(s[:-len(suffix)]) * mult)
            except ValueError:
                return 0
    return 0


def execute_one_run(spec: RunSpec) -> tuple[float, int, float, str]:
    """Execute one full run for the given spec.

    Returns (peak_memory_mb, exit_status, wall_clock_seconds, notes).

    Pandas: one subprocess per load_date, wall_clock summed, peak_memory maxed.
    PySpark: one subprocess for all load_dates, wall_clock = that subprocess.
    """
    t0 = time.perf_counter()
    total_peak_bytes = 0
    aggregate_exit = 0
    aggregate_notes = ""

    if spec.implementation == "pandas":
        # Pandas can only process one partition per invocation. Loop
        # and aggregate. This is the realistic shape of pandas
        # workloads: one process, one chunk, no inter-partition
        # parallelism.
        for load_date in spec.load_dates:
            peak_bytes, exit_code, _wall_ns, notes = _run_pandas_partition(
                spec, load_date,
            )
            if peak_bytes > total_peak_bytes:
                total_peak_bytes = peak_bytes
            if exit_code != 0 and aggregate_exit == 0:
                aggregate_exit = exit_code
                aggregate_notes = notes
                break

    elif spec.implementation == "pyspark":
        # PySpark processes all partitions in one Spark session. No
        # loop, no per-partition JVM cold-start tax.
        peak_bytes, exit_code, _wall_ns, notes = _run_pyspark_all(spec)
        total_peak_bytes = peak_bytes
        aggregate_exit = exit_code
        aggregate_notes = notes

    else:
        raise ValueError(f"unknown implementation: {spec.implementation}")

    wall_seconds = time.perf_counter() - t0
    peak_mb = total_peak_bytes / (1024 * 1024)
    return (peak_mb, aggregate_exit, wall_seconds, aggregate_notes)


# ---------------------------------------------------------------
# CSV append (creates file with header if absent)
# ---------------------------------------------------------------
def _append_csv_rows(csv_path: Path, rows: list[RunResult]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if needs_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())


# ---------------------------------------------------------------
# Baseline driver (Pass 0)
# ---------------------------------------------------------------
@dataclass
class BaselineConfig:
    runs: int = 3
    warmup: int = 1
    scales: list[str] = field(default_factory=lambda: ["small", "medium", "large"])
    implementations: list[str] = field(default_factory=lambda: ["pandas", "pyspark"])
    bronze_dir: Path = Path("data/bronze")
    output_root: Path = Path("data/perf/pass0")
    csv_path: Path = Path("data/perf/perf_runs.csv")
    pandas_large_timeout_seconds: float = 60.0


def _memory_source_for(implementation: str) -> str:
    return {
        "pandas":  "psutil_rss",
        "pyspark": "docker_stats_rss",
    }[implementation]


def _timeout_for(scale: str, implementation: str, cfg: BaselineConfig) -> float | None:
    if implementation == "pandas" and scale == "large":
        return cfg.pandas_large_timeout_seconds
    return None


def _purge_output(output_root: Path, implementation: str) -> None:
    """Clear the prior run's output so writes don't accumulate.

    Each (implementation, run_index) pair would otherwise pile up
    duplicate Parquet files under the same partition dir, which
    poisons future Pass-1 reads.
    """
    impl_dir = output_root / implementation
    if impl_dir.exists():
        shutil.rmtree(impl_dir)


def run_baseline(cfg: BaselineConfig) -> list[RunResult]:
    """Run Pass 0 across all (scale, implementation) combinations.

    Sequential by design — see Phase 4 lesson about Spark stack
    instability under concurrent load.
    """
    results: list[RunResult] = []

    for scale in cfg.scales:
        if scale not in SCALES:
            raise ValueError(f"unknown scale: {scale}")
        load_dates = SCALES[scale]

        for implementation in cfg.implementations:
            log.info("==== %s / %s (%d partitions) ====",
                     scale, implementation, len(load_dates))

            timeout = _timeout_for(scale, implementation, cfg)
            memory_source = _memory_source_for(implementation)

            # Warmup runs: discarded, but we still wipe output between
            # them so timing isn't tainted by overwrites.
            for w in range(cfg.warmup):
                _purge_output(cfg.output_root, implementation)
                log.info("[warmup %d/%d] %s/%s", w + 1, cfg.warmup, scale, implementation)
                spec = RunSpec(
                    implementation=implementation,
                    load_dates=load_dates,
                    bronze_dir=cfg.bronze_dir,
                    output_root=cfg.output_root,
                    timeout_seconds=timeout,
                    run_label=f"warmup-{scale}-{implementation}",
                )
                execute_one_run(spec)

            # Measured runs
            for r in range(1, cfg.runs + 1):
                _purge_output(cfg.output_root, implementation)
                log.info("[run %d/%d] %s/%s", r, cfg.runs, scale, implementation)
                spec = RunSpec(
                    implementation=implementation,
                    load_dates=load_dates,
                    bronze_dir=cfg.bronze_dir,
                    output_root=cfg.output_root,
                    timeout_seconds=timeout,
                    run_label=f"pass0-{scale}-{implementation}-r{r}",
                )
                peak_mb, exit_code, wall_s, notes = execute_one_run(spec)
                result = RunResult(
                    benchmark_name="pass0_baseline",
                    scale=scale,
                    implementation=implementation,
                    run_index=r,
                    wall_clock_seconds=wall_s,
                    peak_memory_mb=peak_mb,
                    memory_source=memory_source,
                    exit_status=exit_code,
                    notes=notes,
                    run_timestamp=datetime.now(timezone.utc).isoformat(),
                )
                results.append(result)
                log.info("    wall=%.2fs peak_mem=%.1fMB exit=%d notes=%s",
                         wall_s, peak_mb, exit_code, notes or "ok")

            # Append after each (scale, implementation) so a mid-run
            # crash doesn't lose finished data.
            _append_csv_rows(cfg.csv_path, results[-cfg.runs:])

    return results


def _print_medians(results: list[RunResult]) -> None:
    """Print median wall-clock + peak memory per (scale, implementation)."""
    buckets: dict[tuple[str, str], list[RunResult]] = {}
    for r in results:
        buckets.setdefault((r.scale, r.implementation), []).append(r)

    print()
    print(f"{'scale':<8} {'impl':<8} {'wall_med_s':>11} {'mem_med_mb':>11} "
          f"{'exit_modal':>11} {'mem_source':>20}")
    print("-" * 72)
    for (scale, impl), rs in sorted(buckets.items()):
        walls = [r.wall_clock_seconds for r in rs if r.exit_status == 0]
        mems = [r.peak_memory_mb for r in rs if r.exit_status == 0]
        exits = [r.exit_status for r in rs]
        modal_exit = statistics.mode(exits) if exits else "?"
        wall_med = statistics.median(walls) if walls else float("nan")
        mem_med = statistics.median(mems) if mems else float("nan")
        mem_src = rs[0].memory_source if rs else "?"
        print(f"{scale:<8} {impl:<8} {wall_med:>11.3f} {mem_med:>11.1f} "
              f"{modal_exit!s:>11} {mem_src:>20}")


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="RiskFlow performance benchmark harness",
    )
    sub = p.add_subparsers(dest="subcommand", required=True)

    b = sub.add_parser("baseline", help="Pass 0: pandas vs PySpark baseline")
    b.add_argument("--runs", type=int, default=3,
                   help="measured runs per (scale, implementation)")
    b.add_argument("--warmup", type=int, default=1,
                   help="discarded warmup runs per (scale, implementation)")
    b.add_argument("--scales", type=str, default="small,medium,large",
                   help="comma-separated scales: small,medium,large")
    b.add_argument("--implementations", type=str, default="pandas,pyspark",
                   help="comma-separated: pandas,pyspark")
    b.add_argument("--bronze-dir", type=Path, default=Path("data/bronze"))
    b.add_argument("--output-root", type=Path, default=Path("data/perf/pass0"))
    b.add_argument("--csv-path", type=Path, default=Path("data/perf/perf_runs.csv"))
    b.add_argument("--pandas-large-timeout-seconds", type=float, default=60.0)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.subcommand == "baseline":
        if psutil is None:
            log.error("psutil is required for the perf harness. "
                      "Install with: pip install psutil")
            return 2
        cfg = BaselineConfig(
            runs=args.runs,
            warmup=args.warmup,
            scales=[s.strip() for s in args.scales.split(",") if s.strip()],
            implementations=[i.strip() for i in args.implementations.split(",") if i.strip()],
            bronze_dir=args.bronze_dir,
            output_root=args.output_root,
            csv_path=args.csv_path,
            pandas_large_timeout_seconds=args.pandas_large_timeout_seconds,
        )
        results = run_baseline(cfg)
        _print_medians(results)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
