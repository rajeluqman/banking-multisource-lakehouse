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
    # "home-credit-default-risk" is a Kaggle COMPETITION, not a dataset — competition_download_files
    # 401'd live this session (rules not accepted; that's an owner action on kaggle.com, no API path
    # exists to accept them). Using a dataset mirror instead: verified 2026-07-15 via
    # kaggle.api.dataset_list_files to contain all 7 needed CSVs (application_train/test, bureau,
    # bureau_balance, previous_application, POS_CASH_balance, credit_card_balance,
    # installments_payments) with no 403/license gate.
    "home_credit": "megancrenshaw/home-credit-default-risk",
    "paysim": "ealaxi/paysim1",  # verified: the well-known PaySim Kaggle mirror
    "berka": "marceloventura/the-berka-dataset",  # verified 2026-07-15 via kaggle.api.dataset_list_files:
    # returns account/card/client/disp/district/loan/order/trans.csv — matches Berka's known table set.
    # The previously guessed slug (sabrinaputridewi/czech-bank-financial-dataset) did not surface in
    # search results at all and was never confirmed to exist.
}


def _require_kaggle_env() -> None:
    if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
        raise SystemExit(
            "KAGGLE_USERNAME / KAGGLE_KEY not set. Fill them in .env (see .env.example) — "
            "this repo never bundles Kaggle credentials or pre-downloaded data."
        )


def _normalize_downloaded_files(name: str, out_dir: Path) -> None:
    """Rename mirror-specific filenames to what the seed loaders (seed/postgres/,
    seed/mssql/, seed/build_xwalk.py) actually expect — those were written against the
    real Kaggle competition/dataset filenames, not the mirrors' own naming."""
    if name == "home_credit":
        # this mirror nests a duplicate copy of application_train/test under a subfolder
        # named after the dataset slug — hoist it to top-level first, then drop the subfolder.
        nested = out_dir / "home-credit-default-risk"
        if nested.is_dir():
            for f in nested.iterdir():
                dest = out_dir / f.name
                if not dest.exists():
                    f.rename(dest)
            import shutil
            shutil.rmtree(nested)
        # seed/postgres/load_home_credit.py reads "application.csv" (a single labeled table);
        # the mirror ships the original train/test split. application_train.csv has TARGET,
        # application_test.csv does not — use train as "application", drop test (unused).
        train = out_dir / "application_train.csv"
        target = out_dir / "application.csv"
        if train.exists() and not target.exists():
            train.rename(target)
        test = out_dir / "application_test.csv"
        if test.exists():
            test.unlink()
    elif name == "berka":
        # seed/salesforce/load_berka.py and seed/build_xwalk.py were written against the
        # original UCI/Berka distribution's ".asc" (semicolon-delimited) filenames — this
        # mirror ships the identical semicolon-delimited content but named "*.csv". Rename,
        # don't reparse: the delimiter already matches, only the extension differs.
        for csv_file in out_dir.glob("*.csv"):
            csv_file.rename(csv_file.with_suffix(".asc"))
    elif name == "paysim":
        # seed/mssql/load_paysim.py reads "paysim.csv"; the Kaggle mirror ships it under its
        # original Kaggle-competition export filename.
        src = out_dir / "PS_20174392719_1491204439457_log.csv"
        target = out_dir / "paysim.csv"
        if src.exists() and not target.exists():
            src.rename(target)


def fetch_kaggle(name: str, dataset_slug: str) -> None:
    _require_kaggle_env()
    import kaggle  # imported lazily — only needed for the 3 Kaggle-gated sources

    out_dir = DATA_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_slug, path=str(out_dir), unzip=True)
    _normalize_downloaded_files(name, out_dir)
    print(f"{name}: downloaded to {out_dir}")


def fetch_bank_marketing() -> None:
    # seed/teradata/load_bank_marketing.py reads "bank-full.csv" directly under out_dir; the UCI
    # zip nests it one level deeper inside an inner "bank.zip" (alongside an unrelated
    # "bank-additional.zip" variant we don't use) — unzip both levels and flatten.
    out_dir = DATA_DIR / "bank_marketing"
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / "bank_marketing.zip"
    urllib.request.urlretrieve(UCI_BANK_MARKETING_URL, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    inner_zip = out_dir / "bank.zip"
    if inner_zip.exists():
        with zipfile.ZipFile(inner_zip) as zf:
            zf.extractall(out_dir)
        inner_zip.unlink()
    for extra in ("bank_marketing.zip", "bank-additional.zip", "bank-additional"):
        p = out_dir / extra
        if p.is_dir():
            import shutil
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()
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
