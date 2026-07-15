#!/usr/bin/env python3
"""Bronze -> Silver, PaySim ("Credit Card + Fraud") domain (ADR-007 D7.1 — split out of the
former build_silver.py so a fraud-pipeline failure never blocks the other 4 domains). Covers
`card_txn` only.

Not executed against live Bronze data this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.common.lake_paths import layer_path
from pipeline.silver.common import mask_last4, merge_upsert


def build_sil_card_txn(spark: SparkSession) -> None:
    """PaySim -> `sil_card_txn`. `isFraud` is the Gold KPI label; `isFlaggedFraud` is kept
    for rule-performance analysis ONLY (R-08) — this builder does not conflate them, and no
    downstream Gold model may read isFlaggedFraud as the fraud KPI (architect veto,
    journey/06_DQ_PLAN.md)."""
    df = spark.read.format("delta").load(layer_path("bronze", "mssql", "paysim_transactions"))
    df = df.withColumnRenamed("isFraud", "is_fraud").withColumnRenamed("isFlaggedFraud", "is_flagged_fraud")
    df = df.withColumnRenamed("type", "txn_type")  # journey/05_STTM.md sil_card_txn.txn_type <- PaySim type
    df = mask_last4(df, "nameOrig").withColumnRenamed("nameOrig", "name_orig_masked")
    merge_upsert(spark, df, "silver", "card_txn", "txn_id")


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("silver_fraud")
    build_sil_card_txn(spark)
    print("silver_fraud complete: 1 table.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
