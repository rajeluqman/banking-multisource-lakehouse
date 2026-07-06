#!/usr/bin/env python3
"""MS SQL Server ("Credit Card + Fraud") -> Landing, high-watermark batch (ADR-004,
unchanged by ADR-006, R-10). Thin driver — extraction logic lives in jdbc_batch_common.py.

Run on Databricks / local Spark, NOT executed in this planning session (no live DB here).
"""

from __future__ import annotations

import os

from pipeline.common.spark_session import get_spark
from pipeline.extract.jdbc_batch_common import extract_table

TABLES = ["paysim_transactions"]


def jdbc_url() -> str:
    host = os.environ.get("MSSQL_HOST", "localhost")
    port = os.environ.get("MSSQL_PORT", "1433")
    db = os.environ.get("MSSQL_DB", "banking_cards")
    return f"jdbc:sqlserver://{host}:{port};databaseName={db};encrypt=true;trustServerCertificate=true"


def main() -> int:
    spark = get_spark("mssql_extract")
    props = {
        "user": os.environ.get("MSSQL_USER", "sa"),
        "password": os.environ["MSSQL_PASSWORD"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    for table in TABLES:
        path = extract_table(spark, "mssql", table, jdbc_url(), props)
        print(f"mssql.{table} -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
