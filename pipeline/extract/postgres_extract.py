#!/usr/bin/env python3
"""Postgres ("Sales / Loan Dept") -> Landing, high-watermark batch (ADR-004, unchanged by
ADR-006). Thin driver — extraction logic lives in jdbc_batch_common.py.

Run on Databricks / local Spark, NOT executed in this planning session (no live DB here).
"""

from __future__ import annotations

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


def main() -> int:
    spark = get_spark("postgres_extract")
    props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    for table in TABLES:
        path = extract_table(spark, "postgres", table, jdbc_url(), props)
        print(f"postgres.{table} -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
