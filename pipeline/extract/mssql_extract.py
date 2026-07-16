#!/usr/bin/env python3
"""MS SQL Server ("Credit Card + Fraud") -> Landing, high-watermark batch (ADR-004,
unchanged by ADR-006, R-10). Thin driver — extraction logic lives in jdbc_batch_common.py.

Run on Databricks / local Spark, NOT executed in this planning session (no live DB here).

Run:  python pipeline/extract/mssql_extract.py [--full-backfill]
"""

from __future__ import annotations

import argparse
import os

from pipeline.common.spark_session import get_spark
from pipeline.extract.jdbc_batch_common import extract_table

TABLES = ["paysim_transactions"]


def jdbc_url() -> str:
    host = os.environ.get("MSSQL_HOST", "localhost")
    port = os.environ.get("MSSQL_PORT", "1433")
    db = os.environ.get("MSSQL_DB", "banking_cards")
    return f"jdbc:sqlserver://{host}:{port};databaseName={db};encrypt=true;trustServerCertificate=true"


def main(full_backfill: bool = False) -> int:
    """`full_backfill` defaults to False so this still conforms to the zero-arg `main() -> int`
    contract pipeline/orchestrate.py relies on — see postgres_extract.py's main() docstring
    for why the `--full-backfill` flag is parsed only in the `__main__` guard, not here."""
    spark = get_spark("mssql_extract")
    props = {
        "user": os.environ.get("MSSQL_USER", "sa"),
        "password": os.environ["MSSQL_PASSWORD"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    for table in TABLES:
        path = extract_table(spark, "mssql", table, jdbc_url(), props, full_backfill=full_backfill)
        print(f"mssql.{table} -> {path}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-backfill", action="store_true",
                     help="force a full re-pull of every table regardless of watermark state (ADR-007 D7.4 Strategy 1)")
    args = ap.parse_args()
    _rc = main(full_backfill=args.full_backfill)
    if _rc != 0:
        raise SystemExit(_rc)
