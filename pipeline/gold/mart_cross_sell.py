#!/usr/bin/env python3
"""`mart_cross_sell` (BQ-06) — healthy-deposit, active, no-card/loan customers, ranked by
prior campaign responsiveness (ADR-006 D6.4). Grain: one row per qualifying customer_id.

"Healthy" balance + "active" (90-day) thresholds: journey/03_DATA_REQUIREMENTS.md — frozen
at Silver build time, not recomputed per mart run. "Balance" is read from `trans` (each
Berka transaction carries the running post-transaction balance — there is no static balance
column on `account` itself; this is the standard Berka schema shape, (unverified) until
checked against the real .asc file per journey/03's assumption log).

`current_balance`/`balance_p50` are reported in MYR (D-12) via
`pipeline/gold/common.py::latest_balance_per_account`'s `current_balance_myr` — Berka's raw
CZK balance was previously surfaced in Gold as-is, not normalized to the reporting currency
(real, live bug — BUILD_REPORT.md §16). Single-source (Berka only) so this was never a
cross-currency-mixing bug like `mart_daily_flows.py`/`mart_customer_360.py`, just a
D-12-compliance gap."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_percentile, col, lit

from pipeline.common.lake_paths import layer_path
from pipeline.gold.common import latest_balance_per_account

ACTIVE_WINDOW_DAYS = 90


def build(spark: SparkSession) -> None:
    dim_customer = spark.read.format("delta").load(layer_path("gold", "dim_customer"))
    fact_loan = spark.read.format("delta").load(layer_path("gold", "fact_loan_application"))
    fact_txn = spark.read.format("delta").load(layer_path("gold", "fact_txn"))
    campaign = spark.read.format("delta").load(layer_path("silver", "campaign_response"))
    disp = spark.read.format("delta").load(layer_path("silver", "disp"))
    trans = spark.read.format("delta").load(layer_path("silver", "trans"))
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "berka") \
        .select(col("native_key").alias("client_id"), "customer_id")

    balances = latest_balance_per_account(spark, trans)
    balance_p50 = balances.select(approx_percentile("current_balance_myr", 0.5).alias("p50")).collect()[0]["p50"]

    deposits = (
        disp.join(balances, "account_id", "left")
        .join(xwalk, "client_id", "left")
        .select("customer_id", col("current_balance_myr").alias("current_balance"))
    )

    has_loan = fact_loan.select("customer_id").distinct().withColumn("has_loan", lit(True))
    has_card = fact_txn.filter(col("source_system") == "paysim").select("customer_id").distinct() \
        .withColumn("has_card", lit(True))
    last_activity = fact_txn.groupBy("customer_id").agg({"txn_ts": "max"}).withColumnRenamed("max(txn_ts)", "last_txn_ts")

    candidates = (
        dim_customer.select("customer_id")
        .join(deposits, "customer_id", "left")
        .join(has_loan, "customer_id", "left")
        .join(has_card, "customer_id", "left")
        .join(last_activity, "customer_id", "left")
        .join(campaign.select("customer_id", "prior_campaign_outcome", "subscribed_term_deposit"), "customer_id", "left")
        .filter(col("has_loan").isNull() & col("has_card").isNull())
        .filter(col("current_balance") >= balance_p50)
    )

    mart = candidates.select(
        "customer_id", "current_balance", "last_txn_ts", "prior_campaign_outcome", "subscribed_term_deposit",
    )
    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_cross_sell"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_cross_sell"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
