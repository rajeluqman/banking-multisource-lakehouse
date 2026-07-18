#!/usr/bin/env python3
"""`fact_account_balance` (ADR-005 Addendum #4) — current-balance snapshot per Berka account,
promoted to a real Gold table so the dbt-on-Snowflake serving layer reads Gold ONLY, never
Silver `trans`. Grain: one row per `account_id` (journey/04_DATA_MODEL.md). Overwrite snapshot
(current-state) — re-materialized each run, no history.

Materializes the existing `pipeline/gold/common.py::latest_balance_per_account` helper (Berka's
`account` has no static balance column — the latest `trans` row per account carries the running
post-transaction balance). Reports both native `current_balance` (CZK) and `current_balance_myr`
(D-12 reporting standard, converted once via the shared helper — no second FX path). `currency`
is retained for lineage. `mart_daily_flows` (BQ-08) and `mart_cross_sell` (BQ-06) consume this."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.common.lake_paths import layer_path
from pipeline.gold.common import latest_balance_per_account


def build(spark: SparkSession) -> None:
    trans = spark.read.format("delta").load(layer_path("silver", "trans"))
    balances = latest_balance_per_account(spark, trans) \
        .select("account_id", "current_balance", "current_balance_myr", "currency")
    balances.write.format("delta").mode("overwrite").save(layer_path("gold", "fact_account_balance"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_account_balance"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
