#!/usr/bin/env python3
"""Seed SAP HANA Cloud ("Internal CRM") from the Berka CSVs, + CDC scaffolding (ADR-006).

Loads all 8 Berka tables (client, account, disp, card, loan, trans, district — the 8th is
the crosswalk, built separately by seed/build_xwalk.py), adds PK + created_at/updated_at
(D-03.1), rebases the 1993-1998 date range to seed day (R-13, D-03.2). Then creates a
`_cdc_log` shadow table + AFTER INSERT/UPDATE/DELETE triggers per table (ADR-006 D6.3) —
this is what makes SAP HANA the CDC-connector showcase source instead of a plain batch pull.

Berka's `birth_number` is loaded VERBATIM here (D-05 — Bronze/raw stays verbatim); the
DOB/gender decode (R-12) happens at Silver, not at seed.

Env: SAP_HANA_HOST/PORT/USER/PASSWORD (see .env.example) — owner-provisioned, never run
against a placeholder. Does not download the CSVs itself — run scripts/fetch_datasets.py
first, and provision the HANA Cloud instance (journey/07_PIPELINE_SPEC.md prerequisites)
before running this against a live instance.

Run:  python seed/sap_hana/load_berka.py --data-dir data/raw/berka [--sample N]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from hdbcli import dbapi

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from seed.common.cdc_ddl import setup_cdc
from seed.common.seeding_utils import SEED_DAY, rebase_dates, seeded_random

# table -> (native PK column, date columns needing D-03.2 rebase)
TABLES = {
    "district": ("district_id", []),
    "client": ("client_id", []),          # birth_number carries its own encoded date (R-12) — not rebased here
    "account": ("account_id", ["date"]),
    "disp": ("disp_id", []),
    "card": ("card_id", ["issued"]),
    "loan": ("loan_id", ["date"]),
    "trans": ("trans_id", ["date"]),
}


def _connection():
    return dbapi.connect(
        address=os.environ["SAP_HANA_HOST"],
        port=int(os.environ.get("SAP_HANA_PORT", 443)),
        user=os.environ["SAP_HANA_USER"],
        password=os.environ["SAP_HANA_PASSWORD"],
        encrypt=True,
    )


def _load_table(conn, data_dir: Path, table: str, pk_column: str, date_columns: list[str], sample: int | None) -> int:
    asc_path = data_dir / f"{table}.asc"
    if not asc_path.exists():
        raise FileNotFoundError(f"{asc_path} not found — run scripts/fetch_datasets.py first "
                                 f"(the docs are a MAP, this file is the TERRITORY).")
    df = pd.read_csv(asc_path, sep=";")
    if sample and len(df) > sample:
        rng = seeded_random(f"berka.{table}")
        df = df.sample(n=sample, random_state=rng.randint(0, 2**31)).reset_index(drop=True)

    for col in date_columns:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], format="%y%m%d", errors="coerce").dt.date
            df[col] = rebase_dates(parsed.dropna().tolist()) if parsed.notna().any() else parsed

    df["created_at"] = SEED_DAY
    df["updated_at"] = SEED_DAY
    df["is_deleted"] = False

    cur = conn.cursor()
    cur.execute(f'DROP TABLE "{table}"' if _table_exists(conn, table) else "SELECT 1 FROM DUMMY")
    columns_sql = ", ".join(f'"{c}" NVARCHAR(200)' for c in df.columns)
    cur.execute(f'CREATE COLUMN TABLE "{table}" ({columns_sql}, PRIMARY KEY ("{pk_column}"))')
    placeholders = ", ".join(["?"] * len(df.columns))
    cur.executemany(f'INSERT INTO "{table}" VALUES ({placeholders})',
                     df.astype(str).values.tolist())
    conn.commit()

    setup_cdc(conn, table, pk_column)  # ADR-006 D6.3 — _cdc_log + triggers, right after seeding
    return len(df)


def _table_exists(conn, table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM SYS.TABLES WHERE TABLE_NAME = ?", (table.upper(),))
    return cur.fetchone()[0] > 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw/berka"))
    ap.add_argument("--sample", type=int, default=None,
                     help="deterministic subset size per table for the dev loop (D-14); omit for full load")
    args = ap.parse_args()

    conn = _connection()
    counts = {}
    for table, (pk_column, date_columns) in TABLES.items():
        counts[table] = _load_table(conn, args.data_dir, table, pk_column, date_columns, args.sample)
        print(f"  {table}: {counts[table]} rows loaded + CDC scaffolding created")

    print(f"Berka/SAP HANA Cloud seed complete: {sum(counts.values())} total rows across {len(TABLES)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
