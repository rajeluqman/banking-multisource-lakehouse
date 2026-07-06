#!/usr/bin/env python3
"""Seed MS SQL Server ("Credit Card + Fraud") from the PaySim CSV (D-03).

PaySim's `step` is a simulation-hour index (1-744), not a real date (R-06) — converted to
a real timestamp via base_date + step*1h, then the whole series is rebased so max(txn_ts)
= seed day (D-03.2/3). Every row gets a generated PK + created_at/updated_at (D-03.1).

Env: MSSQL_HOST/PORT/DB/USER/PASSWORD (see .env.example). Does not download the CSV
itself — run scripts/fetch_datasets.py first.

Run:  python seed/mssql/load_paysim.py --data-dir data/raw/paysim [--sample N]
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import uuid
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from seed.common.seeding_utils import SEED_DAY, seeded_random

BASE_DATE = SEED_DAY - dt.timedelta(days=31)  # PaySim's 744 steps ~= 31 days of simulated hours


def _engine():
    user = os.environ.get("MSSQL_USER", "sa")
    pwd = os.environ["MSSQL_PASSWORD"]
    host = os.environ.get("MSSQL_HOST", "localhost")
    port = os.environ.get("MSSQL_PORT", "1433")
    db = os.environ.get("MSSQL_DB", "banking_cards")
    return create_engine(
        f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw/paysim"))
    ap.add_argument("--sample", type=int, default=None,
                     help="deterministic subset size for the dev loop (D-14); omit for the full 6.3M rows")
    args = ap.parse_args()

    csv_path = args.data_dir / "paysim.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found — run scripts/fetch_datasets.py first.")

    df = pd.read_csv(csv_path)
    if args.sample and len(df) > args.sample:
        rng = seeded_random("paysim")
        df = df.sample(n=args.sample, random_state=rng.randint(0, 2**31)).reset_index(drop=True)

    df["txn_id"] = [str(uuid.uuid5(uuid.NAMESPACE_OID, f"paysim:{i}")) for i in df.index]
    df["txn_ts"] = df["step"].apply(lambda s: dt.datetime.combine(BASE_DATE, dt.time.min) + dt.timedelta(hours=int(s)))
    df["currency"] = "MYR"  # D-12 — PaySim amounts are unitless in the source; tagged, not invented
    df["created_at"] = SEED_DAY
    df["updated_at"] = SEED_DAY
    df["is_deleted"] = False

    engine = _engine()
    df.to_sql("paysim_transactions", engine, if_exists="replace", index=False, chunksize=5000)
    print(f"PaySim seed complete: {len(df)} rows loaded into paysim_transactions.")
    print(f"  base_date={BASE_DATE}, max txn_ts={df['txn_ts'].max()} (rebased to seed day, R-06/D-03.2/3)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
