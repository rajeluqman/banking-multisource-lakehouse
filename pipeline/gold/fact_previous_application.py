#!/usr/bin/env python3
"""`fact_previous_application` (ADR-005 Addendum #4) — Home Credit PRIOR loan applications
promoted from Silver to a Gold event fact so the dbt-on-Snowflake serving layer reads Gold
ONLY, never Silver. Grain: one row per `SK_ID_PREV` (journey/04_DATA_MODEL.md). No SCD —
a prior-application record is an append/snapshot event.

This is a DIFFERENT loan population from `fact_loan_application` — each customer's history of
OTHER prior loans (its own `SK_ID_PREV` grain, ~4.9 rows per `SK_ID_CURR`), carrying the
approval/decision fields (`NAME_CONTRACT_STATUS`, `DAYS_DECISION`) that the current-application
fact does not have. `mart_loan_funnel` (BQ-04) computes its approval-rate / days-to-decision
PROXY from this, joined to `fact_loan_application`'s month cohort by `SK_ID_CURR`.

`SK_ID_CURR`->`customer_id` resolution via `dim_customer_xwalk` is done HERE in the builder
(ADR-005 single identity path), so dbt never touches the crosswalk. `sk_id_curr` is kept for
the mart's cohort join to `fact_loan_application`."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "home_credit") \
        .select(col("native_key").alias("sk_id_curr_str"), "customer_id")
    previous = spark.read.format("delta").load(layer_path("silver", "previous_application")) \
        .withColumn("sk_id_curr_str", col("SK_ID_CURR").cast("string"))

    fact = (
        previous.join(xwalk, "sk_id_curr_str", "left")
        .select(
            col("SK_ID_PREV").alias("sk_id_prev"),
            "customer_id",
            col("SK_ID_CURR").alias("sk_id_curr"),
            col("NAME_CONTRACT_STATUS").alias("name_contract_status"),
            col("DAYS_DECISION").alias("days_decision"),
        )
    )
    fact.write.format("delta").mode("overwrite").save(layer_path("gold", "fact_previous_application"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_previous_application"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
