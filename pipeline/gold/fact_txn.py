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
    # card_txn.name_orig_masked is last-4-masked at Silver (D-07) and can never match the
    # xwalk's native_key (built from Bronze's unmasked nameOrig) — live-caught: this joined
    # to nothing, 100% NULL customer_id for every PaySim row. Resolve identity via Bronze's
    # raw nameOrig instead (never persisted into Gold, only used transiently to look up
    # customer_id), then join that resolution back to Silver by txn_id — same "resolve
    # identity, mask everything else" discipline the Berka leg already follows via
    # client_id (R-38/D-07).
    paysim_bronze = spark.read.format("delta").load(layer_path("bronze", "mssql", "paysim_transactions")) \
        .select(col("txn_id").alias("_txn_id"), col("nameOrig").alias("name_id"))
    txn_customer = paysim_bronze.join(paysim_xwalk, "name_id", "left").select("_txn_id", "customer_id")
    paysim_facts = (
        card_txn.join(txn_customer, card_txn.txn_id == txn_customer._txn_id, "left")
        .select(
            col("txn_id"), col("customer_id"), col("txn_ts"), col("txn_type"),
            col("amount"), col("currency"), col("is_fraud"),
            lit("paysim").alias("source_system"),
        )
    )

    berka_xwalk = xwalk.filter(col("source_system") == "berka") \
        .select(col("native_key").alias("client_id"), "customer_id")
    trans = spark.read.format("delta").load(layer_path("silver", "trans"))
    # account_id -> client_id bridge (N:N, not a CTE) — but a jointly-held account has TWO
    # disp rows (OWNER + DISPONENT). Joining trans to the full bridge fans out every one of
    # that account's transactions once per disponent, silently double-counting `amount` in
    # every downstream mart. Restrict to OWNER (Berka: exactly one per account) so each
    # transaction attributes to a single customer, preserving fact_txn's stated one-row-per-
    # transaction grain.
    disp = spark.read.format("delta").load(layer_path("silver", "disp")).filter(col("type") == "OWNER")
    berka_facts = (
        trans.alias("trans").join(disp.alias("disp"), "account_id", "left")
        .join(berka_xwalk, "client_id", "left")
        .select(
            col("trans_id").alias("txn_id"), col("customer_id"),
            col("date").alias("txn_ts"), col("trans.type").alias("txn_type"),
            col("amount"), lit("CZK").alias("currency"),
            lit(0).cast("long").alias("is_fraud"),  # match sil_card_txn's is_fraud (long 0/1, not boolean)
            lit("berka").alias("source_system"),
        )
    )

    fact = paysim_facts.unionByName(berka_facts)
    fact = fact.withColumn("txn_year", year(col("txn_ts"))).withColumn("txn_month", month(col("txn_ts")))
    fact.write.format("delta").partitionBy("txn_year", "txn_month").mode("append").save(layer_path("gold", "fact_txn"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_txn"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
