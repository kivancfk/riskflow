# data/perf/

Output directory for the Phase 5 perf harness.

This directory is kept under version control via `.gitkeep` so the path
exists before the first `make perf-baseline` run. The harness will
create:

  perf_runs.csv         — append-only log of every measured run
  pass0/                — Pass 0 intermediate output (silver_pandas/, etc.)
  pass1/                — Pass 1 intermediate output (Days 3+)
  pass2/                — Pass 2 intermediate output (Days 4+)
  pass3/                — Pass 3 intermediate output (Day 5)

These intermediate output dirs should be gitignored. perf_runs.csv is
the artifact we commit — it's the source of truth for everything in
docs/performance.md.
