#!/usr/bin/env python3
"""Postgres ("Sales / Loan Dept") -> Landing, high-watermark batch (ADR-004, unchanged by
ADR-006). Thin driver — extraction logic lives in jdbc_batch_common.py.

Run on Databricks / local Spark, NOT executed in this planning session (no live DB here).

Run:  python pipeline/extract/postgres_extract.py [--full-backfill]
"""

from __future__ import annotations

import argparse
import os

from pipeline.common.spark_session import get_spark
from pipeline.extract.jdbc_batch_common import extract_table

TABLES = [
    "application", "bureau", "bureau_balance", "previous_application",
    "pos_cash_balance", "credit_card_balance", "installments_payments",
]


def jdbc_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "banking_sales")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def main(full_backfill: bool = False) -> int:
    """`full_backfill` defaults to False so this still conforms to the zero-arg `main() -> int`
    contract pipeline/orchestrate.py relies on to invoke every stage uniformly — the
    `--full-backfill` CLI flag is parsed only in the `__main__` guard below, never via
    argparse reading `sys.argv` inside `main()` itself (that would swallow the ORCHESTRATOR's
    own argv when it imports and calls this module's `main()` in-process)."""
    spark = get_spark("postgres_extract")
    props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    for table in TABLES:
        path = extract_table(spark, "postgres", table, jdbc_url(), props, full_backfill=full_backfill)
        print(f"postgres.{table} -> {path}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-backfill", action="store_true",
                     help="force a full re-pull of every table regardless of watermark state (ADR-007 D7.4 Strategy 1)")
    args = ap.parse_args()
    _rc = main(full_backfill=args.full_backfill)
    if _rc != 0:
        raise SystemExit(_rc)
