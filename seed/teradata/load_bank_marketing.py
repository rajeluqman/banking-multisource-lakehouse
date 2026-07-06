#!/usr/bin/env python3
"""Seed Teradata ("Marketing / Campaign") from the UCI Bank Marketing dataset, + CDC
scaffolding (ADR-006).

Bank Marketing has no native key linking it to any other source (R-38) — this loader
deterministically samples WITHOUT replacement from the already-built dim_customer_xwalk
(seed/build_xwalk.py must run first) and assigns one customer_id per row. Rows beyond the
xwalk population size are dropped (documented, counted below — not silently truncated).

Then creates the same `_cdc_log` shadow table + AFTER INSERT/UPDATE/DELETE trigger pattern
as SAP HANA Cloud (ADR-006 D6.3, seed/common/cdc_ddl.py) — same shape, reused not
reimplemented.

Env: TERADATA_HOST/USER/PASSWORD (see .env.example) — owner-provisioned, never run
against a placeholder. Does not download the CSV itself — run scripts/fetch_datasets.py
first (UCI Bank Marketing needs no auth), and seed/build_xwalk.py before this.

Run:  python seed/teradata/load_bank_marketing.py --csv data/raw/bank_marketing/bank-full.csv
                                                   --xwalk seed/artifacts/dim_customer_xwalk.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import pandas as pd
import teradatasql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from pipeline.extract.cdc_initial_snapshot import extract_initial_snapshot
from seed.common.cdc_ddl import setup_cdc
from seed.common.seeding_utils import SEED_DAY, seeded_random

BANK_MARKETING_COLUMNS = [
    "age", "job", "marital", "education", "default", "balance", "housing", "loan",
    "contact", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "y",
]


def _connection():
    return teradatasql.connect(
        host=os.environ["TERADATA_HOST"],
        user=os.environ["TERADATA_USER"],
        password=os.environ["TERADATA_PASSWORD"],
    )


def _load_xwalk_customer_ids(xwalk_path: Path) -> list[str]:
    if not xwalk_path.exists():
        raise FileNotFoundError(f"{xwalk_path} not found — run seed/build_xwalk.py first "
                                 f"(Bank Marketing linkage requires the xwalk to exist, R-38).")
    with xwalk_path.open(newline="", encoding="utf-8") as f:
        return sorted({row["customer_id"] for row in csv.DictReader(f)})


def build(csv_path: Path, xwalk_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found — run scripts/fetch_datasets.py first.")
    df = pd.read_csv(csv_path, sep=";", quotechar='"')
    missing = [c for c in BANK_MARKETING_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{csv_path}: expected columns {missing} not found — "
                          f"the docs are a MAP, this file is the TERRITORY; STOP and confirm the real schema.")

    customer_ids = _load_xwalk_customer_ids(xwalk_path)
    rng = seeded_random("bank_marketing_linkage")
    sample_size = min(len(df), len(customer_ids))
    if sample_size < len(df):
        print(f"WARNING: Bank Marketing has {len(df)} rows but xwalk only has "
              f"{len(customer_ids)} customers — dropping {len(df) - sample_size} rows (R-38, not silently truncated)")

    df = df.sample(n=sample_size, random_state=rng.randint(0, 2**31)).reset_index(drop=True)
    assigned_customers = rng.sample(customer_ids, sample_size)  # sample WITHOUT replacement (R-38)
    df.insert(0, "customer_id", assigned_customers)

    df["currency"] = "EUR"  # D-12 — source is a Portuguese bank study
    df["created_at"] = SEED_DAY
    df["updated_at"] = SEED_DAY
    df["is_deleted"] = False
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=Path("data/raw/bank_marketing/bank-full.csv"))
    ap.add_argument("--xwalk", type=Path, default=Path("seed/artifacts/dim_customer_xwalk.csv"))
    args = ap.parse_args()

    df = build(args.csv, args.xwalk)

    conn = _connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE bank_marketing" if _table_exists(conn) else "SELECT 1")
    columns_sql = ", ".join(f'"{c}" VARCHAR(200)' for c in df.columns if c != "customer_id")
    cur.execute(f'CREATE TABLE bank_marketing ("customer_id" VARCHAR(100), {columns_sql}, '
                f'PRIMARY KEY ("customer_id"))')
    placeholders = ", ".join(["?"] * len(df.columns))
    cur.executemany(f"INSERT INTO bank_marketing VALUES ({placeholders})",
                     df.astype(str).values.tolist())
    conn.commit()

    extract_initial_snapshot(df, "teradata", "bank_marketing")  # R-40/ADR-007 D7.5 — land the
    # bulk load into Landing BEFORE triggers exist, so it isn't silently missed by the CDC path.
    setup_cdc(conn, "bank_marketing", "customer_id")  # ADR-006 D6.3
    print(f"Teradata Bank Marketing seed complete: {len(df)} rows loaded + CDC scaffolding created.")
    return 0


def _table_exists(conn) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM DBC.TABLESV WHERE TABLENAME = 'bank_marketing'")
    return cur.fetchone()[0] > 0


if __name__ == "__main__":
    raise SystemExit(main())
