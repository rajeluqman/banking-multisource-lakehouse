#!/usr/bin/env python3
"""Bronze -> Silver, Teradata ("Marketing / Campaign") domain (ADR-007 D7.1 — split out of
the former build_silver.py so a marketing-domain failure never blocks the other 4 domains).
Covers `campaign_response` only.

Not executed against live Bronze data this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.common.lake_paths import layer_path
from pipeline.silver.common import latest_state_from_cdc_log, merge_upsert


def build_sil_campaign_response(spark: SparkSession) -> None:
    """Teradata Bank Marketing (CDC) -> `sil_campaign_response`. No native key of its own
    (R-38) — `customer_id` was assigned at seed time, carried through Bronze verbatim."""
    latest = latest_state_from_cdc_log(spark, "teradata", "bank_marketing_cdc", "customer_id")
    raw = spark.read.format("delta").load(layer_path("bronze", "teradata", "bank_marketing_cdc"))
    full = latest.join(
        raw.select("pk_value", "job", "marital", "education", "default", "balance", "poutcome", "y"),
        latest.customer_id == raw.pk_value,
    ).drop("pk_value")
    df = full.withColumnRenamed("default", "credit_in_default") \
             .withColumnRenamed("balance", "avg_yearly_balance") \
             .withColumnRenamed("poutcome", "prior_campaign_outcome") \
             .withColumnRenamed("y", "subscribed_term_deposit")
    merge_upsert(spark, df, "silver", "campaign_response", "customer_id")


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("silver_marketing")
    build_sil_campaign_response(spark)
    print("silver_marketing complete: 1 table.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
