#!/usr/bin/env python3
"""`fact_crm_case` (ADR-005 Addendum #4) — Salesforce CRM Cases promoted from Silver to a Gold
event fact so the dbt-on-Snowflake serving layer reads Gold ONLY, never Silver. Grain: one row
per Salesforce Case / `case_id` (journey/04_DATA_MODEL.md). No SCD — a Case event is
append/snapshot, it does not re-resolve.

The Berka `client_id`->`customer_id` identity resolution (via `dim_customer_xwalk`) is done
HERE in the Spark builder, NOT downstream — so dbt/Snowflake never touches `dim_customer_xwalk`
or Silver (ADR-005 core: single identity path in Spark; PLAN-dbt-marts-serving-layer.md Step-1
red line). `mart_fraud_followup` (BQ-03) consumes this by joining on `customer_id`.

`case_type` is preserved so the consuming mart can filter to 'Fraud Follow-up' (or any future
case type) itself, rather than baking that filter into the fact — the fact is the full Case
population, one row per Case."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "berka") \
        .select(col("native_key").alias("client_id"), "customer_id")
    crm_case = spark.read.format("delta").load(layer_path("silver", "crm_case"))

    fact = (
        crm_case.join(xwalk, "client_id", "left")
        .select("case_id", "customer_id", "case_type", "opened_at")
    )
    fact.write.format("delta").mode("overwrite").save(layer_path("gold", "fact_crm_case"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_crm_case"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
