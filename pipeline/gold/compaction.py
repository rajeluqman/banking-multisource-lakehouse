#!/usr/bin/env python3
"""Gold table maintenance (ADR-010 decision 2) — closes R-41. Delta `OPTIMIZE` (file
compaction) + `ZORDER BY` (multi-dimensional clustering) as a real, scheduled pipeline
stage. Confirmed unbuilt before this change (zero `OPTIMIZE`/`ZORDER` call sites anywhere in
`pipeline/`) despite `fact_txn`'s partitioned append writes accumulating many small (~4KB)
parquet files per `txn_year`/`txn_month` partition — the exact small-files/no-pruning shape
that makes external-table and Power BI reads slow.

`ZORDER BY customer_id` on `fact_txn` is the ADR-010-named requirement — it turns a
per-customer lookup from a full partition scan into reading only the 1-2 files holding that
customer. Extended here to every other Gold fact/bridge keyed by `customer_id` (or
`account_id` for `fact_account_balance`) for the same reason: these are exactly the tables
Snowflake's external-table point-lookups and the analytics marts join through. Gold
dimensions (`dim_customer`, `dim_customer_xwalk`, `dim_date`, `dim_fx_rate`,
`dim_campaign_response`) are Type-1 overwrite and already single/few-file per run — plain
`OPTIMIZE` (compaction only, no ZORDER) is enough; ZORDERing them would not meaningfully
change read performance and ADR-010 does not name them.

Idempotent and safe to re-run: `OPTIMIZE`/`ZORDER` rewrite files into a new Delta commit,
never touching table content — a table that is already optimized is a fast no-op."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from pipeline.common.lake_paths import layer_path

# (table, zorder_columns) — zorder_columns=() means plain OPTIMIZE, no ZORDER.
MAINTENANCE_TARGETS: list[tuple[str, tuple[str, ...]]] = [
    ("fact_txn", ("customer_id",)),  # ADR-010 decision 2 — named requirement
    ("fact_card_fraud", ("customer_id",)),
    ("fact_loan_application", ("customer_id",)),
    ("fact_crm_case", ("customer_id",)),
    ("fact_previous_application", ("customer_id",)),
    ("fact_account_balance", ("account_id",)),
    ("bridge_customer_account", ("customer_id",)),
    ("fact_repayment_behavior", ("customer_id",)),  # ADR-005 Add #5, BQ-11/HC-1
    ("dim_customer", ()),
    ("dim_customer_xwalk", ()),
    ("dim_campaign_response", ()),
    ("dim_date", ()),
    ("dim_fx_rate", ()),
]


def build(spark: SparkSession) -> None:
    for table, zorder_cols in MAINTENANCE_TARGETS:
        path = layer_path("gold", table)
        if not DeltaTable.isDeltaTable(spark, path):
            # A table not yet deployed to this environment (e.g. dev-loop fixtures missing
            # one of the ADR-005 Add #4 promotions) is skipped, not a hard failure — maintenance
            # runs against whatever Gold tables actually exist.
            continue
        dt = DeltaTable.forPath(spark, path)
        optimizer = dt.optimize()
        if zorder_cols:
            optimizer.executeZOrderBy(*zorder_cols)
        else:
            optimizer.executeCompaction()


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("gold_compaction"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
