#!/usr/bin/env python3
"""Build seed/artifacts/fx_rates.csv — the static FX seed table (D-12, journey/05_STTM.md
"Transform conventions"; ADR-005 addendum #1).

Same "generated at seed time, not committed (`*.csv` is gitignored)" pattern as
`seed/build_xwalk.py` — a fresh checkout must be able to regenerate this file before
`pipeline/gold/dim_fx_rate.py` can load it.

Rates are static and illustrative (D-12: "BNM OpenAPI is an optional live enrich, never a
build dependency") — NOT fetched from any live FX source. `unitless` is a deliberate
non-convertible sentinel (Home Credit's `AMT_INCOME_TOTAL`, anonymized data with no known
real-world currency) — `rate_to_myr` NULL, so `pipeline/gold/common.py::to_myr` produces a
NULL converted amount rather than a silently wrong number.

Run:  python seed/build_fx_rates.py --out seed/artifacts/fx_rates.csv
"""

from __future__ import annotations

import argparse
import csv

RATE_AS_OF = "2026-07-05"

FX_RATES = [
    {
        "currency_code": "MYR",
        "rate_to_myr": "1.0",
        "rate_as_of": RATE_AS_OF,
        "note": "Reporting currency baseline (D-12) - identity rate",
    },
    {
        "currency_code": "CZK",
        "rate_to_myr": "0.205",
        "rate_as_of": RATE_AS_OF,
        "note": "Static illustrative CZK->MYR rate for Berka amounts - not a live BNM feed "
                "(D-12 says BNM OpenAPI is optional live enrich, never a build dependency)",
    },
    {
        "currency_code": "EUR",
        "rate_to_myr": "5.00",
        "rate_as_of": RATE_AS_OF,
        "note": "Static illustrative EUR->MYR rate for Teradata bank_marketing "
                "(Portuguese-bank-study source, assumed EUR per journey/05_STTM.md)",
    },
    {
        "currency_code": "unitless",
        "rate_to_myr": "",
        "rate_as_of": RATE_AS_OF,
        "note": "Non-convertible sentinel - Home Credit AMT_INCOME_TOTAL (anonymized "
                "competition data, no real currency known); tagged per D-12 so R-14's "
                "completeness gate can pass honestly, deliberately NOT converted (NULL "
                "rate -> NULL amount_myr, never a silently wrong number)",
    },
]


def build(out_path: str) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["currency_code", "rate_to_myr", "rate_as_of", "note"])
        writer.writeheader()
        writer.writerows(FX_RATES)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="seed/artifacts/fx_rates.csv")
    args = parser.parse_args()
    build(args.out)
    print(f"wrote {len(FX_RATES)} FX rate(s) to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
