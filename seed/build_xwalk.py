#!/usr/bin/env python3
"""Build dim_customer_xwalk — the MDM keystone (D-04, ADR-005).

Reads native keys directly out of the raw source files (no DB round-trip needed — the
keys already exist in the CSVs before any DB load happens). Home Credit is the largest,
most complete population and anchors one customer_id per SK_ID_CURR. Berka and PaySim
customer-shaped identifiers are then deterministically resolved: some fraction overlap
onto an EXISTING Home Credit customer_id (simulating "this Berka client is also a
Home Credit loan holder"), the rest get their own new customer_id. This manufactured
overlap is what makes Customer 360 (BQ-01) meaningful — without it, no customer would
ever appear in more than one source and the crosswalk would be hollow.

Grain: one row per (customer_id, source_system) pair (journey/04_DATA_MODEL.md) — a
customer_id CAN have multiple rows (one per source it participates in).

PaySim merchant accounts (nameOrig/nameDest starting with "M") are deliberately EXCLUDED
from the crosswalk (R-09) — merchants are not bank customers.

Run:  python seed/build_xwalk.py --data-dir data/raw --out seed/artifacts/dim_customer_xwalk.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from seed.common.seeding_utils import seeded_random

# Golden-record survivorship order (ADR-005 Add #2). `obp: 2` is a RESERVED/WITHDRAWN tier — OBP
# is deliberately Silver-terminal and is NEVER seeded into the xwalk (no obp rows are appended
# below; line ~139 filters it out of the by_source tally). The rank is left at 2 rather than
# renumbering home_credit→2/paysim→3 so the existing seeded ranks (1/3/4) stay reproducible; only
# relative order matters (dim_customer.py Window.orderBy). Effective seeded tiers: berka=1,
# home_credit=3, paysim=4.
SOURCE_PRIORITY = {"berka": 1, "obp": 2, "home_credit": 3, "paysim": 4}

# Fraction of a source's customer-shaped identifiers that overlap onto an EXISTING
# bank-wide customer_id rather than minting a new one — tunable, not load-bearing logic.
BERKA_OVERLAP_FRACTION = 0.5
PAYSIM_OVERLAP_FRACTION = 0.4


def _read_column(csv_path: Path, column: str) -> list[str]:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} not found — run scripts/fetch_datasets.py first "
            f"(this repo does not download datasets automatically)."
        )
    # Berka's ".asc" files are semicolon-delimited (same format seed/salesforce/load_berka.py
    # reads with sep=";"); Home Credit/PaySim ".csv" files are comma-delimited.
    delimiter = ";" if csv_path.suffix == ".asc" else ","
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if column not in (reader.fieldnames or []):
            raise ValueError(f"{csv_path}: expected column '{column}' not found — "
                              f"the docs are a MAP, this file is the TERRITORY; STOP and confirm the real schema.")
        return [row[column] for row in reader if row[column]]


def build(data_dir: Path, paysim_sample: int | None = None) -> list[dict]:
    rng = seeded_random("build_xwalk")
    rows: list[dict] = []
    customer_pool: list[str] = []  # bank-wide customer_ids minted so far, for overlap sampling

    # 1. Home Credit anchors the identity space — every SK_ID_CURR gets its own new customer_id.
    home_credit_ids = _read_column(data_dir / "home_credit" / "application.csv", "SK_ID_CURR")
    for sk_id in home_credit_ids:
        cust_id = f"CUST_{sk_id}"
        customer_pool.append(cust_id)
        rows.append({"customer_id": cust_id, "source_system": "home_credit",
                     "native_key": sk_id, "source_priority_rank": SOURCE_PRIORITY["home_credit"]})

    # 2. Berka clients: some fraction overlap onto an existing Home Credit customer (this Berka
    #    client is ALSO a loan holder), the rest mint a new Berka-exclusive customer_id.
    berka_ids = _read_column(data_dir / "berka" / "client.asc", "client_id")
    for client_id in berka_ids:
        if customer_pool and rng.random() < BERKA_OVERLAP_FRACTION:
            cust_id = rng.choice(customer_pool)
        else:
            cust_id = f"CUST_BK_{client_id}"
            customer_pool.append(cust_id)
        rows.append({"customer_id": cust_id, "source_system": "berka",
                     "native_key": client_id, "source_priority_rank": SOURCE_PRIORITY["berka"]})

    # 3. PaySim customer-shaped identifiers (C-prefixed only — R-09 excludes M-prefixed merchants):
    #    some fraction overlap onto an existing customer, rest mint a new PaySim-exclusive one.
    #    At dev-loop scale, sampled from the exact same ROWS `seed/mssql/load_paysim.py`
    #    actually loads into MSSQL (same `seeded_random("paysim")` namespace + identical
    #    pandas `.sample()` call) — NOT an independent re-sample. Live-caught: this file used
    #    to sample unique customer IDs under a DIFFERENT RNG namespace
    #    (`"build_xwalk.paysim_sample"`), so the xwalk's PaySim population and MSSQL's actual
    #    seeded rows were two unrelated draws from the same 6.36M-row pool — `fact_txn.py`'s
    #    PaySim leg resolved only ~62/20000 rows to a `customer_id` (almost exactly the ~63
    #    expected by chance for two independent 20k samples), silently breaking every mart
    #    that resolves a PaySim transaction to a bank-wide customer (R-38/D-03.4 — a rebuild
    #    from scratch must be reproducible/consistent, not two independent samples).
    paysim_csv_path = data_dir / "paysim" / "paysim.csv"
    if not paysim_csv_path.exists():
        raise FileNotFoundError(
            f"{paysim_csv_path} not found — run scripts/fetch_datasets.py first "
            f"(this repo does not download datasets automatically)."
        )
    paysim_df = pd.read_csv(paysim_csv_path)
    if paysim_sample and len(paysim_df) > paysim_sample:
        paysim_row_rng = seeded_random("paysim")  # SAME namespace as seed/mssql/load_paysim.py
        paysim_df = paysim_df.sample(n=paysim_sample, random_state=paysim_row_rng.randint(0, 2**31)).reset_index(drop=True)
    paysim_raw = paysim_df["nameOrig"].astype(str).tolist() + paysim_df["nameDest"].astype(str).tolist()
    paysim_customer_ids = sorted({n for n in paysim_raw if n.startswith("C")})
    for name_id in paysim_customer_ids:
        if customer_pool and rng.random() < PAYSIM_OVERLAP_FRACTION:
            cust_id = rng.choice(customer_pool)
        else:
            cust_id = f"CUST_PS_{name_id}"
            customer_pool.append(cust_id)
        rows.append({"customer_id": cust_id, "source_system": "paysim",
                     "native_key": name_id, "source_priority_rank": SOURCE_PRIORITY["paysim"]})

    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    ap.add_argument("--out", type=Path, default=Path("seed/artifacts/dim_customer_xwalk.csv"))
    ap.add_argument("--paysim-sample", type=int, default=None,
                     help="row-count sample matching seed/mssql/load_paysim.py's --sample "
                          "(same value, same RNG namespace) for the dev loop (D-14); omit "
                          "for the full population")
    args = ap.parse_args()

    rows = build(args.data_dir, paysim_sample=args.paysim_sample)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "source_system", "native_key", "source_priority_rank"])
        writer.writeheader()
        writer.writerows(rows)

    # single pass, not the O(unique_customers * rows) rescan this used to do — that's fine at
    # fixture-test scale (41 rows, R-23) but pathological at real Kaggle-data scale (300K+ rows).
    by_source = {s: 0 for s in SOURCE_PRIORITY if s != "obp"}
    sources_by_customer: dict[str, set[str]] = {}
    for r in rows:
        by_source[r["source_system"]] += 1
        sources_by_customer.setdefault(r["customer_id"], set()).add(r["source_system"])
    unique_customers = sources_by_customer.keys()
    multi_source = sum(1 for sources in sources_by_customer.values() if len(sources) > 1)
    print(f"dim_customer_xwalk: {len(rows)} rows, {len(unique_customers)} unique bank-wide customers")
    for source, count in by_source.items():
        print(f"  rows from {source}: {count}")
    print(f"  customers present in >1 source: {multi_source}")
    print("  obp: linked at Silver, not seeded — OBP is a live sandbox (journey/04_DATA_MODEL.md)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
