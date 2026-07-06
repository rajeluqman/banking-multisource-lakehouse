#!/usr/bin/env python3
"""`fact_txn` — one row per transaction, unioned + conformed across sources
(journey/04_DATA_MODEL.md). Snapshot/append-only grain — a transaction never changes after
landing, so no SCD/MERGE-on-update semantics, just append of new rows resolved to
`customer_id` via `dim_customer_xwalk`.

v1 sources: PaySim (`sil_card_txn`) and Berka (`sil_trans`) — OBP transactions join in the
same shape once the OBP extractor lands data (obp is snapshot-append per journey/01, not a
volume source).

Partitioned by `txn_year`/`txn_month` (ADR-007 D7.4 Strategy 2) — Landing already partitions
by `dt=` (ADR-003); this extends the same query-pruning principle to Gold so Snowflake/Power
BI DirectQuery/Import filters on transaction date without a full-table scan."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, month, year

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk"))

    paysim_xwalk = xwalk.filter(col("source_system") == "paysim") \
        .select(col("native_key").alias("name_id"), "customer_id")
    card_txn = spark.read.format("delta").load(layer_path("silver", "card_txn"))
    paysim_facts = (
        card_txn.join(paysim_xwalk, card_txn.name_orig_masked == paysim_xwalk.name_id, "left")
        .select(
            col("txn_id"), col("customer_id"), col("txn_ts"), col("txn_type"),
            col("amount"), col("currency"), col("is_fraud"),
            lit("paysim").alias("source_system"),
        )
    )

    berka_xwalk = xwalk.filter(col("source_system") == "berka") \
        .select(col("native_key").alias("client_id"), "customer_id")
    trans = spark.read.format("delta").load(layer_path("silver", "trans"))
    disp = spark.read.format("delta").load(layer_path("silver", "disp"))  # account_id -> client_id bridge (N:N, not a CTE)
    berka_facts = (
        trans.join(disp, "account_id", "left")
        .join(berka_xwalk, "client_id", "left")
        .select(
            col("trans_id").alias("txn_id"), col("customer_id"),
            col("date").alias("txn_ts"), col("type").alias("txn_type"),
            col("amount"), lit("CZK").alias("currency"), lit(False).alias("is_fraud"),
            lit("berka").alias("source_system"),
        )
    )

    fact = paysim_facts.unionByName(berka_facts)
    fact = fact.withColumn("txn_year", year(col("txn_ts"))).withColumn("txn_month", month(col("txn_ts")))
    fact.write.format("delta").partitionBy("txn_year", "txn_month").mode("append").save(layer_path("gold", "fact_txn"))


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_txn"))
