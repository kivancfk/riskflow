"""
RiskFlow — split PaySim into daily partitions.

PaySim's `step` column represents one simulated hour. The full dataset
spans 743 steps (~30.96 days). This script slices the source CSV into
day_01.csv, day_02.csv, ... day_30.csv based on step ranges:

    day N → steps [(N-1)*24 + 1 .. N*24]

Steps beyond day 30 (i.e. step > 720) are written to day_30_overflow.csv
so nothing is silently dropped.

Usage:
    python scripts/split_paysim.py \\
        --input  data/raw/paysim.csv \\
        --output data/partitioned

The script is intentionally pandas-based, not PySpark: it runs once,
produces small files, and `make split-data` calls it inside the Airflow
container which already has pandas installed.

Reproducibility: the same input always produces the same outputs.
The only side effect is writing files to --output.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------
HOURS_PER_DAY = 24
DAYS = 30
OVERFLOW_DAY_LABEL = "30_overflow"

REQUIRED_COLUMNS = {
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("split_paysim")


# ---------------------------------------------------------------
# Pure functions (these are what unit tests will cover later)
# ---------------------------------------------------------------
def assign_day(step: int) -> str:
    """Map a PaySim `step` to a day label.

    Day 1 = steps 1..24, day 2 = steps 25..48, ..., day 30 = steps 697..720.
    Anything beyond step 720 is bucketed into the overflow label so it is
    written out but never confused with a real daily partition.

    >>> assign_day(1)
    '01'
    >>> assign_day(24)
    '01'
    >>> assign_day(25)
    '02'
    >>> assign_day(720)
    '30'
    >>> assign_day(721)
    '30_overflow'
    """
    if step < 1:
        raise ValueError(f"step must be >= 1, got {step}")
    if step > DAYS * HOURS_PER_DAY:
        return OVERFLOW_DAY_LABEL
    day_number = (step - 1) // HOURS_PER_DAY + 1
    return f"{day_number:02d}"


def validate_schema(df: pd.DataFrame) -> None:
    """Raise if the input DataFrame is missing any expected PaySim columns."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Input CSV is missing required PaySim columns: {sorted(missing)}. "
            f"Found columns: {sorted(df.columns)}"
        )


# ---------------------------------------------------------------
# Driver
# ---------------------------------------------------------------
def split(input_path: Path, output_dir: Path) -> dict[str, int]:
    """Split the PaySim CSV at `input_path` into per-day files in `output_dir`.

    Returns a dict mapping day-label -> row count for caller logging / asserts.
    """
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input CSV not found at {input_path}. "
            "Download PaySim and place it at data/raw/paysim.csv."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Reading %s ...", input_path)
    df = pd.read_csv(input_path)
    log.info("Loaded %s rows, %s columns", f"{len(df):,}", len(df.columns))

    validate_schema(df)

    # Vectorized day assignment is much faster than apply()
    df["_day"] = df["step"].map(assign_day)

    counts: dict[str, int] = {}
    for day, chunk in df.groupby("_day", sort=True):
        out_path = output_dir / f"day_{day}.csv"
        chunk_to_write = chunk.drop(columns=["_day"])
        chunk_to_write.to_csv(out_path, index=False)
        counts[day] = len(chunk_to_write)
        log.info("wrote %s  (%s rows)", out_path.name, f"{counts[day]:,}")

    # Sanity: total rows out should equal total rows in
    total_out = sum(counts.values())
    if total_out != len(df):
        raise RuntimeError(
            f"Row-count mismatch: read {len(df)} but wrote {total_out}. "
            "Refusing to declare success."
        )

    log.info("Done. %s daily files (+ overflow if any) totaling %s rows.",
             len(counts), f"{total_out:,}")
    return counts


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to the source PaySim CSV (e.g. data/raw/paysim.csv).",
    )
    p.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Directory to write day_XX.csv files into.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        split(args.input, args.output)
    except (FileNotFoundError, ValueError) as e:
        log.error(str(e))
        return 1
    except Exception:
        log.exception("Unexpected failure while splitting PaySim")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
