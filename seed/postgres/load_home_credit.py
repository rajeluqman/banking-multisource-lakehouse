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
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from seed.common.seeding_utils import SEED_DAY, seeded_random

TABLES = [
    "application", "bureau", "bureau_balance", "previous_application",
    "POS_CASH_balance", "credit_card_balance", "installments_payments",
]


def _engine():
    user = os.environ["POSTGRES_USER"]
    pwd = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "banking_sales")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")


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

    df.to_sql(table.lower(), engine, if_exists="replace", index=False, chunksize=5000)
    return len(df)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw/home_credit"))
    ap.add_argument("--sample", type=int, default=None,
                     help="deterministic subset size per table for the dev loop (D-14); omit for full load")
    args = ap.parse_args()

    engine = _engine()
    counts = {}
    for table in TABLES:
        counts[table] = load_table(engine, args.data_dir, table, args.sample)
        print(f"  {table}: {counts[table]} rows loaded")

    print(f"Home Credit seed complete: {sum(counts.values())} total rows across {len(TABLES)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
