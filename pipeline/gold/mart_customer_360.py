#!/usr/bin/env python3
"""`mart_customer_360` (BQ-01) — product count by type + total relationship value per
customer_id, reported in MYR (D-12). Grain: one row per customer_id
(journey/04_DATA_MODEL.md).

`total_txn_value` was NOT actually normalized before this session's fix (real, live bug —
BUILD_REPORT.md §16): any customer with both a Berka (CZK) and a PaySim (MYR) transaction —
real multi-source customers exist in `dim_customer_xwalk` — got the two currencies silently
summed together. Now sums `fact_txn.amount_myr` (converted once at the fact layer)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, count, lit, sum as spark_sum, when

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    dim_customer = spark.read.format("delta").load(layer_path("gold", "dim_customer"))
    fact_txn = spark.read.format("delta").load(layer_path("gold", "fact_txn"))
    fact_loan = spark.read.format("delta").load(layer_path("gold", "fact_loan_application"))
    campaign = spark.read.format("delta").load(layer_path("silver", "campaign_response"))

    txn_agg = fact_txn.groupBy("customer_id").agg(
        count("*").alias("txn_count"), spark_sum("amount_myr").alias("txn_value"),
    )
    loan_agg = fact_loan.groupBy("customer_id").agg(count("*").alias("loan_count"))
    deposit_flag = campaign.select(
        "customer_id",
        when(col("subscribed_term_deposit") == True, 1).otherwise(0).alias("has_term_deposit"),  # noqa: E712
    )

    mart = (
        dim_customer.select("customer_id")
        .join(txn_agg, "customer_id", "left")
        .join(loan_agg, "customer_id", "left")
        .join(deposit_flag, "customer_id", "left")
        .select(
            "customer_id",
            coalesce(col("txn_count"), lit(0)).alias("txn_count"),
            coalesce(col("loan_count"), lit(0)).alias("loan_count"),
            coalesce(col("has_term_deposit"), lit(0)).alias("has_term_deposit"),
            coalesce(col("txn_value"), lit(0.0)).alias("total_txn_value"),
        )
        .withColumn(
            "product_count",
            (col("loan_count") > 0).cast("int") + (col("txn_count") > 0).cast("int") + col("has_term_deposit"),
        )
    )
    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_customer_360"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_customer_360"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
