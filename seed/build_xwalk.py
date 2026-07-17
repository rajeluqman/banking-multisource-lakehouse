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

Memory note (D-14 canonical run, full 6.36M-row PaySim CSV, ~471MB): `build()` is a
GENERATOR — rows are yielded one at a time in the exact same Home Credit -> Berka -> PaySim
order (and hence the exact same `rng` call sequence, see PAYSIM_OVERLAP_FRACTION comment
below) a prior list-building version produced; `main()` streams each row straight to the
output CSV instead of accumulating ~7.2M dicts in memory first. The PaySim identifier
population itself is computed by `_paysim_customer_ids()` without ever materializing the
full 11-column DataFrame or a concatenated 12.7M-element Python list — see that function's
docstring. This is a memory-safety fix only; output is byte-for-byte identical to an
unconstrained-memory run of the original algorithm (same RNG order, same dedup+sort logic).

Run:  python seed/build_xwalk.py --data-dir data/raw --out seed/artifacts/dim_customer_xwalk.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections.abc import Iterator
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

# Row-chunk size for streaming the full PaySim CSV — keeps peak memory bounded regardless of
# how large the source file is (only 2 of PaySim's 11 columns are ever loaded per chunk).
# Not load-bearing: chunk boundaries cannot change the final deduped/sorted identifier set
# (see _paysim_customer_ids docstring), so this is a perf/memory knob only.
PAYSIM_CHUNK_SIZE = 500_000


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


def _paysim_customer_ids(paysim_csv_path: Path, paysim_sample: int | None) -> list[str]:
    """Unique C-prefixed (customer-shaped — R-09 excludes M-prefixed merchants) identifiers
    across nameOrig+nameDest, sorted. Produces the SAME final list a full in-memory
    `df["nameOrig"].astype(str).tolist() + df["nameDest"].astype(str).tolist()` then
    `sorted({n for n in ... if n.startswith("C")})` would (set-union + sort is order-independent
    of how the members were discovered), but without ever holding the full 11-column DataFrame
    or a concatenated 12.7M-element list in memory:
      - `usecols=["nameOrig", "nameDest"]` — the only two columns build() ever reads from
        paysim_df — cuts loaded columns from 11 to 2 regardless of which branch runs below.
      - When no `--paysim-sample` cap applies (the canonical D-14 full-population run), the
        file is streamed via `chunksize` and identifiers are folded into a running `set`
        instead of two full `.tolist()` calls concatenated into one big list first.
    """
    ids: set[str] = set()

    def _absorb(frame: pd.DataFrame) -> None:
        name_orig = frame["nameOrig"].astype(str)
        name_dest = frame["nameDest"].astype(str)
        ids.update(name_orig[name_orig.str.startswith("C")])
        ids.update(name_dest[name_dest.str.startswith("C")])

    if paysim_sample:
        # Sampling needs the row population addressable by position to match
        # seed/mssql/load_paysim.py's pandas `.sample(random_state=...)` call exactly.
        # `.sample()` selects by row position, not by column content, so trimming to
        # nameOrig/nameDest via usecols cannot change which rows it picks — reproducibility
        # is preserved. This branch is the small dev-loop path (D-14 dev loop), not the
        # OOM-triggering full-population path, so a single (2-column) read is fine here.
        paysim_df = pd.read_csv(paysim_csv_path, usecols=["nameOrig", "nameDest"])
        if len(paysim_df) > paysim_sample:
            paysim_row_rng = seeded_random("paysim")  # SAME namespace as seed/mssql/load_paysim.py
            paysim_df = paysim_df.sample(
                n=paysim_sample, random_state=paysim_row_rng.randint(0, 2**31)
            ).reset_index(drop=True)
        _absorb(paysim_df)
    else:
        for chunk in pd.read_csv(paysim_csv_path, usecols=["nameOrig", "nameDest"],
                                  chunksize=PAYSIM_CHUNK_SIZE):
            _absorb(chunk)

    return sorted(ids)


def build(data_dir: Path, paysim_sample: int | None = None) -> Iterator[dict]:
    """Yields dim_customer_xwalk rows one at a time — Home Credit, then Berka, then PaySim,
    the same order (and hence the same `rng` call sequence) a prior list-returning version
    produced. Nothing is buffered into a giant in-memory list here; callers (main()) stream
    each row straight to the output CSV as it's produced (D-14 full-population memory fix)."""
    rng = seeded_random("build_xwalk")
    customer_pool: list[str] = []  # bank-wide customer_ids minted so far, for overlap sampling

    # 1. Home Credit anchors the identity space — every SK_ID_CURR gets its own new customer_id.
    home_credit_ids = _read_column(data_dir / "home_credit" / "application.csv", "SK_ID_CURR")
    for sk_id in home_credit_ids:
        cust_id = f"CUST_{sk_id}"
        customer_pool.append(cust_id)
        yield {"customer_id": cust_id, "source_system": "home_credit",
               "native_key": sk_id, "source_priority_rank": SOURCE_PRIORITY["home_credit"]}

    # 2. Berka clients: some fraction overlap onto an existing Home Credit customer (this Berka
    #    client is ALSO a loan holder), the rest mint a new Berka-exclusive customer_id.
    berka_ids = _read_column(data_dir / "berka" / "client.asc", "client_id")
    for client_id in berka_ids:
        if customer_pool and rng.random() < BERKA_OVERLAP_FRACTION:
            cust_id = rng.choice(customer_pool)
        else:
            cust_id = f"CUST_BK_{client_id}"
            customer_pool.append(cust_id)
        yield {"customer_id": cust_id, "source_system": "berka",
               "native_key": client_id, "source_priority_rank": SOURCE_PRIORITY["berka"]}

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
    paysim_customer_ids = _paysim_customer_ids(paysim_csv_path, paysim_sample)
    for name_id in paysim_customer_ids:
        if customer_pool and rng.random() < PAYSIM_OVERLAP_FRACTION:
            cust_id = rng.choice(customer_pool)
        else:
            cust_id = f"CUST_PS_{name_id}"
            customer_pool.append(cust_id)
        yield {"customer_id": cust_id, "source_system": "paysim",
               "native_key": name_id, "source_priority_rank": SOURCE_PRIORITY["paysim"]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    ap.add_argument("--out", type=Path, default=Path("seed/artifacts/dim_customer_xwalk.csv"))
    ap.add_argument("--paysim-sample", type=int, default=None,
                     help="row-count sample matching seed/mssql/load_paysim.py's --sample "
                          "(same value, same RNG namespace) for the dev loop (D-14); omit "
                          "for the full population")
    args = ap.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    # Streaming pass: write each row as build() yields it (no ~7.2M-dict `rows` list held in
    # memory at full-PaySim-population scale), while still computing the same summary stats a
    # single post-hoc pass over `rows` used to (D-14 memory fix; stats are byte-for-byte
    # equivalent — see source_bits note below for why).
    by_source = {s: 0 for s in SOURCE_PRIORITY if s != "obp"}
    # Bitmask per customer_id of which sources it's appeared in SO FAR, in place of a
    # `set[str]` per customer (or the old full `rows` list) — far lighter at ~7M-row PaySim
    # scale. Bits only ever accumulate (OR) for a given customer_id, so "crosses from <=1 to
    # >1 distinct sources" can only happen once per customer_id — same final multi_source
    # count a full `len(sources) > 1` scan over the complete row set would produce, computed
    # incrementally instead of needing all rows materialized first.
    source_bits = {s: 1 << i for i, s in enumerate(by_source)}
    seen: dict[str, int] = {}
    multi_source = 0
    row_count = 0

    with args.out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "source_system", "native_key", "source_priority_rank"])
        writer.writeheader()
        for row in build(args.data_dir, paysim_sample=args.paysim_sample):
            writer.writerow(row)
            row_count += 1
            by_source[row["source_system"]] += 1

            cust_id = row["customer_id"]
            bit = source_bits[row["source_system"]]
            prev_bits = seen.get(cust_id, 0)
            new_bits = prev_bits | bit
            if new_bits != prev_bits:
                was_multi = prev_bits.bit_count() > 1
                seen[cust_id] = new_bits
                if not was_multi and new_bits.bit_count() > 1:
                    multi_source += 1

    print(f"dim_customer_xwalk: {row_count} rows, {len(seen)} unique bank-wide customers")
    for source, count in by_source.items():
        print(f"  rows from {source}: {count}")
    print(f"  customers present in >1 source: {multi_source}")
    print("  obp: linked at Silver, not seeded — OBP is a live sandbox (journey/04_DATA_MODEL.md)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
