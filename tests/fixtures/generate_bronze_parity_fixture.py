"""Generate the bronze parity fixture used by test_pandas_pyspark_parity.

This is a one-time script. Commit both this generator and the resulting
.parquet file. The generator exists so reviewers can regenerate the
fixture from source if the bronze schema ever changes.

The fixture is 10 deliberately-chosen rows exercising every branch of
the silver transform:

  Rows 1, 2, 3, 5, 6 — clean rows, expected to land in silver.
  Row 4            — duplicate of row 1 with a later _load_ts.
                     Dedup must keep row 4 (latest by _load_ts).
  Row 7            — amount = 0.00 → quarantine (non_positive_amount).
  Row 8            — null name_dest → quarantine (null:name_dest).
  Row 9            — negative amount → quarantine (non_positive_amount).
  Row 10           — null name_orig → quarantine (null:name_orig).

Expected output:
  silver:     5 rows (1-or-4, 2, 3, 5, 6 in some order)
  quarantine: 4 rows (7, 8, 9, 10)

Bronze schema matches the PaySim camelCase convention that the existing
PySpark silver_transform.py expects. The silver-layer snake_case rename
happens inside cast_financial_columns().

Run:
    python tests/fixtures/generate_bronze_parity_fixture.py
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


# Bronze schema (camelCase, matches what bronze ingest produces).
# Financial columns are stored as float64 in bronze; silver casts them to
# DecimalType(18,2). isFraud/isFlaggedFraud are int in bronze; silver
# casts them to boolean and renames to snake_case.
BRONZE_SCHEMA = pa.schema(
    [
        ("step", pa.int32()),
        ("type", pa.string()),
        ("amount", pa.float64()),
        ("nameOrig", pa.string()),
        ("oldbalanceOrg", pa.float64()),
        ("newbalanceOrig", pa.float64()),
        ("nameDest", pa.string()),
        ("oldbalanceDest", pa.float64()),
        ("newbalanceDest", pa.float64()),
        ("isFraud", pa.int32()),
        ("isFlaggedFraud", pa.int32()),
        ("_load_ts", pa.timestamp("us")),
        ("_source_file", pa.string()),
    ]
)


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


# Each row is a dict matching BRONZE_SCHEMA. We use explicit values
# rather than a builder to keep this dead-readable for reviewers.
ROWS = [
    # Row 1: clean PAYMENT
    dict(step=1, type="PAYMENT", amount=100.00,
         nameOrig="C001", oldbalanceOrg=1000.00, newbalanceOrig=900.00,
         nameDest="M001", oldbalanceDest=0.00, newbalanceDest=0.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:00:00"), _source_file="day_01.csv"),
    # Row 2: clean TRANSFER, marked fraud
    dict(step=2, type="TRANSFER", amount=250.00,
         nameOrig="C003", oldbalanceOrg=500.00, newbalanceOrig=250.00,
         nameDest="C004", oldbalanceDest=0.00, newbalanceDest=250.00,
         isFraud=1, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:01:00"), _source_file="day_01.csv"),
    # Row 3: clean CASH_OUT
    dict(step=3, type="CASH_OUT", amount=50.00,
         nameOrig="C005", oldbalanceOrg=100.00, newbalanceOrig=50.00,
         nameDest="C006", oldbalanceDest=0.00, newbalanceDest=50.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:02:00"), _source_file="day_01.csv"),
    # Row 4: DUPLICATE of row 1 by DEDUP_KEY (name_orig, name_dest, step, amount).
    # Later _load_ts — dedup must keep this one and drop row 1.
    dict(step=1, type="PAYMENT", amount=100.00,
         nameOrig="C001", oldbalanceOrg=1000.00, newbalanceOrig=900.00,
         nameDest="M001", oldbalanceDest=0.00, newbalanceDest=0.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:05:00"), _source_file="day_01.csv"),
    # Row 5: clean DEBIT
    dict(step=4, type="DEBIT", amount=75.50,
         nameOrig="C007", oldbalanceOrg=200.00, newbalanceOrig=124.50,
         nameDest="C008", oldbalanceDest=0.00, newbalanceDest=75.50,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:03:00"), _source_file="day_01.csv"),
    # Row 6: clean CASH_IN
    dict(step=5, type="CASH_IN", amount=1000.00,
         nameOrig="C009", oldbalanceOrg=0.00, newbalanceOrig=1000.00,
         nameDest="C010", oldbalanceDest=5000.00, newbalanceDest=4000.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:04:00"), _source_file="day_01.csv"),
    # Row 7: quarantine — non_positive_amount (amount == 0)
    dict(step=6, type="TRANSFER", amount=0.00,
         nameOrig="C011", oldbalanceOrg=100.00, newbalanceOrig=100.00,
         nameDest="C012", oldbalanceDest=0.00, newbalanceDest=0.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:06:00"), _source_file="day_01.csv"),
    # Row 8: quarantine — null name_dest
    dict(step=7, type="PAYMENT", amount=50.00,
         nameOrig="C013", oldbalanceOrg=100.00, newbalanceOrig=50.00,
         nameDest=None, oldbalanceDest=0.00, newbalanceDest=0.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:07:00"), _source_file="day_01.csv"),
    # Row 9: quarantine — non_positive_amount (amount < 0)
    dict(step=8, type="TRANSFER", amount=-10.00,
         nameOrig="C014", oldbalanceOrg=100.00, newbalanceOrig=110.00,
         nameDest="C015", oldbalanceDest=0.00, newbalanceDest=0.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:08:00"), _source_file="day_01.csv"),
    # Row 10: quarantine — null name_orig
    dict(step=9, type="PAYMENT", amount=200.00,
         nameOrig=None, oldbalanceOrg=100.00, newbalanceOrig=100.00,
         nameDest="C016", oldbalanceDest=0.00, newbalanceDest=200.00,
         isFraud=0, isFlaggedFraud=0,
         _load_ts=_ts("2026-04-15T10:09:00"), _source_file="day_01.csv"),
]


def build_table() -> pa.Table:
    """Build the bronze fixture as a pyarrow Table with the canonical schema."""
    columns = {field.name: [r[field.name] for r in ROWS] for field in BRONZE_SCHEMA}
    return pa.Table.from_pydict(columns, schema=BRONZE_SCHEMA)


def main() -> int:
    out_path = Path(__file__).parent / "bronze_parity_fixture.parquet"
    table = build_table()
    pq.write_table(table, out_path, compression="snappy")
    print(f"Wrote {len(ROWS)} rows to {out_path}")
    print(f"Schema:\n{table.schema}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
