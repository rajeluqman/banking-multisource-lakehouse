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
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path
from pipeline.silver.common import latest_state_from_cdc_log, merge_upsert

_VALUE_COLUMNS = ["job", "marital", "education", "default", "balance", "poutcome", "y", "currency"]


def build_sil_campaign_response(spark: SparkSession) -> None:
    """Teradata Bank Marketing -> `sil_campaign_response`. No native key of its own (R-38)
    — `customer_id` was assigned at seed time, carried through Bronze verbatim.

    UNIONs two Bronze sources (R-40/ADR-007 D7.5): the one-time bulk-seed snapshot
    (`bank_marketing`, landed by `cdc_initial_snapshot.py` since the AFTER-INSERT/UPDATE/
    DELETE triggers only capture changes made AFTER they exist, not the seed-time bulk
    load itself) as the baseline, overlaid with any CDC events from `bank_marketing_cdc`
    for customers touched since seed (update -> latest values win, delete -> excluded
    entirely). Without this UNION, Silver only ever sees customers who happen to have a
    post-seed change — live-confirmed: reading the CDC log alone against a freshly-seeded
    45,211-row table returns 0 Silver rows, since the CDC log has no events at all yet."""
    from delta.tables import DeltaTable

    baseline = (
        spark.read.format("delta").load(layer_path("bronze", "teradata", "bank_marketing"))
        .select("customer_id", *_VALUE_COLUMNS)
    )

    cdc_path = layer_path("bronze", "teradata", "bank_marketing_cdc")
    if DeltaTable.isDeltaTable(spark, cdc_path):
        cdc_raw = spark.read.format("delta").load(cdc_path)
        touched_ids = cdc_raw.select(col("pk_value").alias("customer_id")).distinct()
        latest = latest_state_from_cdc_log(spark, "teradata", "bank_marketing_cdc", "customer_id")  # excludes deletes
        cdc_overlay = (
            latest.join(cdc_raw.select("pk_value", *_VALUE_COLUMNS), latest.customer_id == cdc_raw.pk_value)
            .select("customer_id", *_VALUE_COLUMNS)
        )
        full = baseline.join(touched_ids, "customer_id", "left_anti").unionByName(cdc_overlay)
    else:
        full = baseline

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
