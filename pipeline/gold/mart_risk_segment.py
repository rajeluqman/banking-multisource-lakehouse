#!/usr/bin/env python3
"""`mart_risk_segment` (BQ-05) — default rate by segment, current high-risk ACTIVE
customers. Grain: one row per (customer_id, segment) (journey/04_DATA_MODEL.md).

Segment = income band (journey/03_DATA_REQUIREMENTS.md: LOW/MEDIUM/HIGH by percentile,
frozen at Silver build time) + Bank Marketing job/education enrichment (ADR-006 D6.4).
Surfaces disagreement between Home Credit `TARGET` and Bank Marketing `credit_in_default`
rather than silently picking one (journey/06_DQ_PLAN.md)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_percentile, col, count, lit, when

from pipeline.common.lake_paths import layer_path

ACTIVE_WINDOW_DAYS = 90  # journey/03_DATA_REQUIREMENTS.md — same window as BQ-06/07


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "home_credit") \
        .select(col("native_key").alias("SK_ID_CURR"), "customer_id")
    application = spark.read.format("delta").load(layer_path("silver", "application"))
    campaign = spark.read.format("delta").load(layer_path("silver", "campaign_response"))
    fact_txn = spark.read.format("delta").load(layer_path("gold", "fact_txn"))

    income_bounds = application.select(
        approx_percentile("AMT_INCOME_TOTAL", 0.25).alias("p25"),
        approx_percentile("AMT_INCOME_TOTAL", 0.75).alias("p75"),
    ).collect()[0]

    segmented = application.withColumn(
        "income_band",
        when(col("AMT_INCOME_TOTAL") < income_bounds["p25"], lit("LOW"))
        .when(col("AMT_INCOME_TOTAL") > income_bounds["p75"], lit("HIGH"))
        .otherwise(lit("MEDIUM")),
    )

    last_activity = fact_txn.groupBy("customer_id").agg({"txn_ts": "max"}).withColumnRenamed("max(txn_ts)", "last_txn_ts")

    joined = (
        segmented.join(xwalk.withColumnRenamed("SK_ID_CURR", "sk_str"), segmented.SK_ID_CURR.cast("string") == col("sk_str"), "left")
        .join(campaign, "customer_id", "left")
        .join(last_activity, "customer_id", "left")
        .withColumn(
            "default_disagreement",
            (col("TARGET").cast("int") != col("credit_in_default").cast("int"))
            & col("credit_in_default").isNotNull(),
        )
    )

    mart = joined.groupBy("customer_id", "income_band", "NAME_INCOME_TYPE").agg(
        count(when(col("TARGET") == 1, True)).alias("is_default"),
        count(when(col("default_disagreement"), True)).alias("default_signal_disagreement"),
    )
    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_risk_segment"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_risk_segment"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
