#!/usr/bin/env python3
"""`dim_campaign_response` (ADR-005 Addendum #4) — the Teradata Bank Marketing campaign
response promoted from Silver to a conformed Gold dimension so the dbt-on-Snowflake serving
layer reads Gold ONLY, never Silver (journey/09_SECURITY_AND_ACCESS.md line 69: serving_ro =
Gold external tables only). Grain: one row per `customer_id` (journey/04_DATA_MODEL.md).

Type 1 SCD (overwrite each run) — same as `dim_customer`; a campaign-response snapshot
re-resolves each run, no history in v1.

DELIBERATELY a distinct dimension, NOT folded into `dim_customer` (ADR-005 Add #4): campaign
response is a separate marketing-survey business entity; folding would make `dim_customer` a
mixed-domain dimension (Clean-ERD violation). `sil_campaign_response` is already 1:1 on
`customer_id` (ADR-006 D6.2 assigns `customer_id` at seed, sampled without replacement — no
native key), so this is a straight 1:1 promotion, no aggregation.

**Classification carries over (journey/09 lines 40-41, @scope-guardian ruling 2026-07-18):**
`credit_in_default` (risk), `job`/`marital`/`education` (confidential/demographic) keep their
restricted classification AS a Gold table — becoming Gold does not launder RBAC. This dim is
scoped to BQ-05/06-facing roles (risk/marketing), not the general analyst/serving surface."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    campaign = spark.read.format("delta").load(layer_path("silver", "campaign_response"))
    dim = campaign.select(
        "customer_id", "job", "marital", "education", "credit_in_default",
        "avg_yearly_balance", "prior_campaign_outcome", "subscribed_term_deposit",
    )
    dim.write.format("delta").mode("overwrite").save(layer_path("gold", "dim_campaign_response"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("dim_campaign_response"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
