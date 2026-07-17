#!/usr/bin/env python3
"""Seed PostgreSQL ("Sales / Loan Dept") from the Home Credit Default Risk CSVs (D-03).

Loads all 7 relational tables verbatim-columns (R-02 — Bronze/Silver do the pruning, not
seeding), adds PK + created_at/updated_at to every table (D-03.1). Home Credit has no
native timestamp columns (R-01), so created_at = updated_at = seed day for every row at
seed time; drip_feed.py is what advances updated_at on simulated changes.

Env: POSTGRES_HOST/PORT/DB/USER/PASSWORD (see .env.example). Does not download the CSVs
itself — run scripts/fetch_datasets.py first.

Run:  python seed/postgres/load_home_credit.py --data-dir data/raw/home_credit [--sample N]
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from seed.common.seeding_utils import SEED_DAY, seeded_random

TABLES = [
    "application", "bureau", "bureau_balance", "previous_application",
    "POS_CASH_balance", "credit_card_balance", "installments_payments",
]

# 2026-07-17, @finops ruling (row-count ground truth, not CSV file size — bureau_balance is a
# narrow 3-column table, smaller file than installments_payments but 2x the row count):
# these 3 tables get capped at 2M rows on a full/canonical (no --sample) run; the other 4 run
# at their true scale (largest is credit_card_balance at 3.84M rows, no cap needed).
LARGE_TABLE_CAPS = {"bureau_balance": 2_000_000, "installments_payments": 2_000_000, "POS_CASH_balance": 2_000_000}


_copy_chunk_counter = 0


def _psql_insert_copy(table, conn, keys, data_iter):
    """Postgres COPY-based bulk insert — pandas' own documented recipe for `to_sql`'s `method`
    param. Default row-by-row executemany was far too slow for PaySim's 6.36M rows (session 8,
    same day) even on MSSQL's bulk-copy-capable driver; Postgres's native COPY is the
    equivalent fast path here, needed for tables up to 3.84M rows (uncapped) / 2M (capped).

    Prints a periodic heartbeat (every 20 chunks = 100K rows at the default chunksize=5000) —
    live-observed (2026-07-17) that a fully silent multi-minute call dies early/unpredictably
    in this environment regardless of stated timeout, while calls with periodic flushed stdout
    (same pattern already proven for PaySim's chunked loader) complete reliably."""
    global _copy_chunk_counter
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        buf = StringIO()
        writer = csv.writer(buf)
        rows = list(data_iter)
        writer.writerows(rows)
        buf.seek(0)
        columns = ", ".join(f'"{k}"' for k in keys)
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name
        cur.copy_expert(sql=f"COPY {table_name} ({columns}) FROM STDIN WITH CSV", file=buf)
    _copy_chunk_counter += 1
    if _copy_chunk_counter % 20 == 0:
        print(f"    ...{_copy_chunk_counter * 5000:,}+ rows copied so far", flush=True)


def _engine():
    user = os.environ["POSTGRES_USER"]
    pwd = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "banking_sales")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}") # secrets-scan:allow — built from env vars, not a literal secret


INSERT_SLICE_SIZE = 800_000  # 2026-07-17, live-caught: a single to_sql call inserting
# previous_application (1.67M rows x 37 wide columns) died consistently between 1M-1.67M rows,
# while bureau_balance (2M rows x 3 narrow columns) succeeded fine in one shot — points to total
# serialized byte volume in this environment, not row count. 800K rows of even a wide table
# stayed safely under it in testing (confirmed up to 1M). Slicing the INSERT (not the CSV read,
# which was never the problem) into bounded pieces makes every table's load robust regardless of
# width, without needing to fit an exact byte-volume theory.


def load_table(engine, data_dir: Path, table: str, sample: int | None) -> int:
    csv_path = data_dir / f"{table}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found — run scripts/fetch_datasets.py first "
                                 f"(the docs are a MAP, this file is the TERRITORY).")
    df = pd.read_csv(csv_path)
    if sample and len(df) > sample:
        rng = seeded_random(f"home_credit.{table}")
        df = df.sample(n=sample, random_state=rng.randint(0, 2**31)).reset_index(drop=True)

    df["created_at"] = SEED_DAY
    df["updated_at"] = SEED_DAY
    df["is_deleted"] = False

    total = len(df)
    for start in range(0, total, INSERT_SLICE_SIZE):
        slice_df = df.iloc[start:start + INSERT_SLICE_SIZE]
        slice_df.to_sql(
            table.lower(), engine, if_exists="replace" if start == 0 else "append",
            index=False, chunksize=5000, method=_psql_insert_copy,
        )
        print(f"  {table}: {min(start + INSERT_SLICE_SIZE, total):,}/{total:,} rows inserted so far", flush=True)
    return total


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw/home_credit"))
    ap.add_argument("--sample", type=int, default=None,
                     help="deterministic subset size for EVERY table (dev loop, D-14); omit for "
                          "the full/canonical run, which still applies LARGE_TABLE_CAPS per-table")
    args = ap.parse_args()

    engine = _engine()
    counts = {}
    for table in TABLES:
        # --sample (if given) overrides uniformly, dev-loop style; otherwise only the 3
        # @finops-flagged large tables get capped, everything else runs at true scale.
        effective_sample = args.sample if args.sample is not None else LARGE_TABLE_CAPS.get(table)
        counts[table] = load_table(engine, args.data_dir, table, effective_sample)
        print(f"  {table}: {counts[table]} rows loaded")

    print(f"Home Credit seed complete: {sum(counts.values())} total rows across {len(TABLES)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
