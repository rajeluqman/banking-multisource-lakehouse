#!/usr/bin/env python3
"""`fact_card_fraud` — one row per PaySim transaction flagged `isFraud=1` (journey/04). Only
`is_fraud` is used as the KPI label; `is_flagged_fraud` stays in Silver for rule-performance
analysis only and is NEVER read here (R-08, architect veto if it ever is).

Partitioned by `txn_year`/`txn_month` (ADR-007 D7.4 Strategy 2), same as `fact_txn` — Gold
facts extend Landing's `dt=` partition-pruning principle (ADR-003) so downstream query
engines don't full-scan a growing fraud table."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year

from pipeline.common.lake_paths import layer_path
from pipeline.gold.common import to_myr


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "paysim") \
        .select(col("native_key").alias("name_id"), "customer_id")
    card_txn = spark.read.format("delta").load(layer_path("silver", "card_txn"))
    # card_txn.name_orig_masked is last-4-masked at Silver (D-07) and can never match the
    # xwalk's native_key (built from Bronze's unmasked nameOrig) — same bug class fixed in
    # fact_txn.py (live-caught there: 100% NULL customer_id). Resolve identity via Bronze's
    # raw nameOrig instead (never persisted into Gold), joined back to Silver by txn_id.
    paysim_bronze = spark.read.format("delta").load(layer_path("bronze", "mssql", "paysim_transactions")) \
        .select(col("txn_id").alias("_txn_id"), col("nameOrig").alias("name_id"))
    txn_customer = paysim_bronze.join(xwalk, "name_id", "left").select("_txn_id", "customer_id")

    fraud = (
        card_txn.filter(col("is_fraud") == True)  # noqa: E712 — Spark boolean column comparison
        .join(txn_customer, card_txn.txn_id == txn_customer._txn_id, "left")
        .select("txn_id", "customer_id", "txn_ts", "txn_type", "amount", "currency")
    )
    # D-12 — PaySim-only today (rate 1.0), but routed through the same fact-layer FX
    # conversion as fact_txn.py rather than special-cased, so R-14 stays uniform if a
    # non-MYR fraud source is ever added.
    fraud = to_myr(spark, fraud, "amount", "currency", "amount_myr")
    fraud = fraud.withColumn("txn_year", year(col("txn_ts"))).withColumn("txn_month", month(col("txn_ts")))
    fraud.write.format("delta").partitionBy("txn_year", "txn_month").mode("append").save(layer_path("gold", "fact_card_fraud"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_card_fraud"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
