#!/usr/bin/env python3
"""`mart_loan_funnel` (BQ-04) — applications/month, approval rate, avg days app->decision.
Grain: one row per application month (journey/04_DATA_MODEL.md).

`previous_application.DAYS_DECISION`/`NAME_CONTRACT_STATUS` column existence verified
2026-07-17 against the real Home Credit Silver schema (journey/03_DATA_REQUIREMENTS.md).
`previous_application` is a DIFFERENT loan population from `application` — each customer's
own history of OTHER prior loans (its own `SK_ID_PREV` grain, ~4.9 rows per `SK_ID_CURR`),
not the current application itself; `application` carries no approval/decision field of its
own. `approval_rate_pct`/`avg_days_to_decision` are therefore an event-weighted PROXY over
each month-cohort's prior-loan population, not a measure of the current application's own
outcome (@staff-data-engineer ruling, journey/08 BQ-04 evidence).

`application_count` is aggregated from `application` alone (its native 1:1 `SK_ID_CURR`
grain) and joined to the `previous_application` aggregate at the reporting grain
(`app_month`) — NOT computed by grouping a single application-join-previous dataframe.
Live-caught real bug (BUILD_REPORT.md, journey/08 BQ-04, 2026-07-17): the old code counted
`count("*")` over `application.join(previous, "SK_ID_CURR", "left")`, a 1:N join that fanned
`application_count` out to 1,430,155 at real Kaggle scale (real distinct applications:
307,511) — invisible at the old ~5,000-row dev-loop sample, only surfaced once Gold ran
against the real full-scale S3 data."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs, avg, col, count, date_format, when

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    application = spark.read.format("delta").load(layer_path("silver", "application"))
    previous = spark.read.format("delta").load(layer_path("silver", "previous_application"))

    apps_by_month = application.withColumn("app_month", date_format(col("created_at"), "yyyy-MM"))
    application_counts = apps_by_month.groupBy("app_month").agg(count("*").alias("application_count"))

    month_lookup = apps_by_month.select("SK_ID_CURR", "app_month")
    approval_events = previous.join(month_lookup, "SK_ID_CURR", "inner").select(
        "app_month",
        when(col("NAME_CONTRACT_STATUS") == "Approved", 1).otherwise(0).alias("approved"),
        spark_abs(col("DAYS_DECISION")).alias("days_to_decision"),
    )
    approval_agg = approval_events.groupBy("app_month").agg(
        (count(when(col("approved") == 1, True)) / count("*") * 100).alias("approval_rate_pct"),
        avg("days_to_decision").alias("avg_days_to_decision"),
    )

    mart = application_counts.join(approval_agg, "app_month", "left")
    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_loan_funnel"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_loan_funnel"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
