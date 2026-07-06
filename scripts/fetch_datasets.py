#!/usr/bin/env python3
"""Download the 4 seed datasets into data/raw/ (gitignored).

NOT run automatically by any other script in this repo, and NOT run by the planning/build
session — per owner instruction, dataset downloads happen in whichever environment
actually executes the pipeline. Run this yourself there.

- Home Credit Default Risk (Kaggle, needs KAGGLE_USERNAME/KAGGLE_KEY)
- PaySim (Kaggle, needs KAGGLE_USERNAME/KAGGLE_KEY)
- Berka / "Czech bank financial dataset" (Kaggle, needs KAGGLE_USERNAME/KAGGLE_KEY)
- UCI Bank Marketing (no auth — direct download, verified reachable: 200 OK)

Run:  python scripts/fetch_datasets.py [--only home_credit|paysim|berka|bank_marketing]
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.request
import zipfile
from pathlib import Path

DATA_DIR = Path("data/raw")

UCI_BANK_MARKETING_URL = "https://archive.ics.uci.edu/static/public/222/bank+marketing.zip"

KAGGLE_DATASETS = {
    "home_credit": "home-credit-default-risk",  # verified: the actual Kaggle competition slug
    "paysim": "ealaxi/paysim1",  # verified: the well-known PaySim Kaggle mirror
    "berka": "sabrinaputridewi/czech-bank-financial-dataset",  # (unverified) — search
    # Kaggle for "Czech bank financial dataset" / "Berka" yourself and correct this slug
    # before running; not fetched or confirmed this session (owner instruction: no
    # downloads from the planning session).
}


def _require_kaggle_env() -> None:
    if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
        raise SystemExit(
            "KAGGLE_USERNAME / KAGGLE_KEY not set. Fill them in .env (see .env.example) — "
            "this repo never bundles Kaggle credentials or pre-downloaded data."
        )


def fetch_kaggle(name: str, dataset_slug: str) -> None:
    _require_kaggle_env()
    import kaggle  # imported lazily — only needed for the 3 Kaggle-gated sources

    out_dir = DATA_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_slug, path=str(out_dir), unzip=True)
    print(f"{name}: downloaded to {out_dir}")


def fetch_bank_marketing() -> None:
    out_dir = DATA_DIR / "bank_marketing"
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / "bank_marketing.zip"
    urllib.request.urlretrieve(UCI_BANK_MARKETING_URL, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    print(f"bank_marketing: downloaded + extracted to {out_dir} (no auth needed, R-38 context)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=list(KAGGLE_DATASETS) + ["bank_marketing"], default=None)
    args = ap.parse_args()

    targets = [args.only] if args.only else list(KAGGLE_DATASETS) + ["bank_marketing"]
    for name in targets:
        if name == "bank_marketing":
            fetch_bank_marketing()
        else:
            fetch_kaggle(name, KAGGLE_DATASETS[name])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
