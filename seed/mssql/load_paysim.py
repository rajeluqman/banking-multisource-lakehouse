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
CHUNK_SIZE = 500_000  # full 6.36M-row load processed in chunks (2026-07-17) — the whole-file
# path (still used for --sample, which downsamples first) held the full 6.36M-row frame PLUS
# full-size derived columns (a 6.36M-element uuid list, a 6.36M-element datetime series) in
# memory at once for the unsampled case; live-observed dying silently (SIGTERM, no partial rows
# ever reaching the DB) well before any duration-based timeout would explain it — consistent
# with memory pressure, not a slow query. Chunking keeps peak memory bounded regardless of scale.


def _engine():
    user = os.environ.get("MSSQL_USER", "sa")
    pwd = os.environ["MSSQL_PASSWORD"]
    host = os.environ.get("MSSQL_HOST", "localhost")
    port = os.environ.get("MSSQL_PORT", "1433")
    db = os.environ.get("MSSQL_DB", "banking_cards")
    return create_engine(
        f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes", # secrets-scan:allow — built from env vars, not a literal secret
        fast_executemany=True,  # default pyodbc row-by-row executemany can't finish PaySim's 6.36M
        # rows in a reasonable window (live-timed out at 10 min); fast_executemany switches to
        # SQL Server's ODBC bulk-copy path, ~10-100x faster for this row count.
    )


def _transform(df: pd.DataFrame, row_offset: int) -> pd.DataFrame:
    """Adds the generated/derived columns (D-03.1/D-03.2/D-12) — shared by both the
    whole-file (`--sample`) and chunked (full-scale) load paths. `row_offset` keeps the
    uuid5 seed globally unique/stable across chunks (equivalent to the old single-pass
    `df.index`, which ran 0..N-1 over the whole file)."""
    df = df.reset_index(drop=True)
    df["txn_id"] = [str(uuid.uuid5(uuid.NAMESPACE_OID, f"paysim:{row_offset + i}")) for i in df.index]
    df["txn_ts"] = df["step"].apply(lambda s: dt.datetime.combine(BASE_DATE, dt.time.min) + dt.timedelta(hours=int(s)))
    df["currency"] = "MYR"  # D-12 — PaySim amounts are unitless in the source; tagged, not invented
    df["created_at"] = SEED_DAY
    df["updated_at"] = SEED_DAY
    df["is_deleted"] = False
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw/paysim"))
    ap.add_argument("--sample", type=int, default=None,
                     help="deterministic subset size for the dev loop (D-14); omit for the full 6.3M rows")
    args = ap.parse_args()

    csv_path = args.data_dir / "paysim.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found — run scripts/fetch_datasets.py first.")

    engine = _engine()

    if args.sample:
        # Dev-loop scale (D-14) — downsample FIRST (bounds memory), then transform+load in one
        # shot; this path is already proven fast (tested up to 2M rows, ~166s).
        df = pd.read_csv(csv_path)
        if len(df) > args.sample:
            rng = seeded_random("paysim")
            df = df.sample(n=args.sample, random_state=rng.randint(0, 2**31)).reset_index(drop=True)
        df = _transform(df, row_offset=0)
        df.to_sql("paysim_transactions", engine, if_exists="replace", index=False, chunksize=5000)
        total, max_ts = len(df), df["txn_ts"].max()
    else:
        # Full 6.36M-row canonical load — chunked so peak memory never holds the whole file's
        # derived columns at once (see CHUNK_SIZE comment above).
        total = 0
        max_ts = None
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE)):
            chunk = _transform(chunk, row_offset=total)
            chunk.to_sql(
                "paysim_transactions", engine, if_exists="replace" if i == 0 else "append",
                index=False, chunksize=5000,
            )
            total += len(chunk)
            chunk_max = chunk["txn_ts"].max()
            max_ts = chunk_max if max_ts is None else max(max_ts, chunk_max)
            print(f"  ...{total} rows loaded so far")

    print(f"PaySim seed complete: {total} rows loaded into paysim_transactions.")
    print(f"  base_date={BASE_DATE}, max txn_ts={max_ts} (rebased to seed day, R-06/D-03.2/3)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
