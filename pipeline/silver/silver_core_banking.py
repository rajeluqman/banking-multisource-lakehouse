#!/usr/bin/env python3
"""Bronze -> Silver, Open Bank Project ("Core Banking API") domain (ADR-007 D7.1 — split
out of the former build_silver.py so a core-banking-domain failure never blocks the other 4
domains). Covers `accounts` and `transactions`, both from the public-view sandbox walk
(pipeline/extract/obp_client.py — public banks -> public accounts -> each account's own
public view's transactions, live-corrected from the original `/my/accounts` assumption
which always returned zero rows for our sandbox user).

Not a plain `build_simple_table` passthrough (unlike silver_sales.py/silver_crm.py's
simple tables) — OBP's own PK field is `id` on both objects, not `account_id`/
`transaction_id`, and `transactions` carries the owning-account FK nested at
`this_account.id`, verbatim JSON shape (R-19) rather than a flat table. Live-verified
against the real sandbox response shape, not assumed."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path
from pipeline.silver.common import mask_last4, merge_upsert


def build_sil_obp_accounts(spark: SparkSession) -> None:
    """OBP `accounts` -> `sil_obp_accounts`. Passthrough, naming only (STTM)."""
    raw = spark.read.format("delta").load(layer_path("bronze", "obp", "accounts"))
    df = raw.select(col("id").alias("account_id"), col("bank_id"), col("label"))
    df = mask_last4(df, "account_id")
    merge_upsert(spark, df, "silver", "obp_accounts", "account_id")


def build_sil_obp_transactions(spark: SparkSession) -> None:
    """OBP `transactions` -> `sil_obp_transactions`. `this_account.id` resolves the owning
    account (FK to `sil_obp_accounts`); `details.value` is this transaction's own amount
    (distinct from `details.new_balance`, the running post-transaction balance)."""
    raw = spark.read.format("delta").load(layer_path("bronze", "obp", "transactions"))
    df = raw.select(
        col("id").alias("transaction_id"),
        col("this_account.id").alias("account_id"),
        col("details.completed").alias("txn_ts"),
        col("details.description").alias("description"),
        col("details.value.amount").cast("double").alias("amount"),
        col("details.value.currency").alias("currency"),
    )
    merge_upsert(spark, df, "silver", "obp_transactions", "transaction_id")


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("silver_core_banking")
    build_sil_obp_accounts(spark)
    build_sil_obp_transactions(spark)
    print("silver_core_banking complete: 2 tables.")
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
