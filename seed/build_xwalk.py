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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from seed.common.seeding_utils import seeded_random

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
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if column not in (reader.fieldnames or []):
            raise ValueError(f"{csv_path}: expected column '{column}' not found — "
                              f"the docs are a MAP, this file is the TERRITORY; STOP and confirm the real schema.")
        return [row[column] for row in reader if row[column]]


def build(data_dir: Path) -> list[dict]:
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
    paysim_raw = (
        _read_column(data_dir / "paysim" / "paysim.csv", "nameOrig")
        + _read_column(data_dir / "paysim" / "paysim.csv", "nameDest")
    )
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
    args = ap.parse_args()

    rows = build(args.data_dir)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "source_system", "native_key", "source_priority_rank"])
        writer.writeheader()
        writer.writerows(rows)

    unique_customers = {r["customer_id"] for r in rows}
    by_source = {s: sum(1 for r in rows if r["source_system"] == s) for s in SOURCE_PRIORITY if s != "obp"}
    multi_source = sum(1 for c in unique_customers
                        if len({r["source_system"] for r in rows if r["customer_id"] == c}) > 1)
    print(f"dim_customer_xwalk: {len(rows)} rows, {len(unique_customers)} unique bank-wide customers")
    for source, count in by_source.items():
        print(f"  rows from {source}: {count}")
    print(f"  customers present in >1 source: {multi_source}")
    print("  obp: linked at Silver, not seeded — OBP is a live sandbox (journey/04_DATA_MODEL.md)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
