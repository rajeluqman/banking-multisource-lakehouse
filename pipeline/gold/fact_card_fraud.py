#!/usr/bin/env python3
"""`fact_card_fraud` — one row per PaySim transaction flagged `isFraud=1` (journey/04). Only
`is_fraud` is used as the KPI label; `is_flagged_fraud` stays in Silver for rule-performance
analysis only and is NEVER read here (R-08, architect veto if it ever is)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "paysim") \
        .select(col("native_key").alias("name_id"), "customer_id")
    card_txn = spark.read.format("delta").load(layer_path("silver", "card_txn"))

    fraud = (
        card_txn.filter(col("is_fraud") == True)  # noqa: E712 — Spark boolean column comparison
        .join(xwalk, card_txn.name_orig_masked == xwalk.name_id, "left")
        .select("txn_id", "customer_id", "txn_ts", "txn_type", "amount", "currency")
    )
    fraud.write.format("delta").mode("append").save(layer_path("gold", "fact_card_fraud"))


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_card_fraud"))
