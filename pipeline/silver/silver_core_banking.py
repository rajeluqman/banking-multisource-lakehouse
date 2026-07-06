#!/usr/bin/env python3
"""Bronze -> Silver, Open Bank Project ("Core Banking API") domain (ADR-007 D7.1 — split
out of the former build_silver.py so a core-banking-domain failure never blocks the other 4
domains). Covers `obp_accounts` and `obp_transactions`, both generic passthrough (naming +
masking only, journey/05_STTM.md).

Not executed against live Bronze data this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.silver.common import build_simple_table

SIMPLE_TABLES = [
    # (source, bronze_table, silver_table, pk_column, mask_columns)
    ("obp", "accounts", "obp_accounts", "account_id", ["account_id"]),
    ("obp", "transactions", "obp_transactions", "transaction_id", []),
]


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("silver_core_banking")
    for source, bronze_table, silver_table, pk_column, mask_columns in SIMPLE_TABLES:
        build_simple_table(spark, source, bronze_table, silver_table, pk_column, mask_columns)
    print(f"silver_core_banking complete: {len(SIMPLE_TABLES)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
