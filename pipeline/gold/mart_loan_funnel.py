#!/usr/bin/env python3
"""`mart_loan_funnel` (BQ-04) — applications/month, approval rate, avg days app->decision.
Grain: one row per application month (journey/04_DATA_MODEL.md).

`previous_application.DAYS_DECISION` is used as the app->decision lag proxy — flagged
(unverified) in journey/03_DATA_REQUIREMENTS.md pending confirmation against the real
Home Credit schema; if that column doesn't exist as named, this must be corrected against
the real file, not silently left wrong (anti-shortcut rule)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs, avg, col, count, date_format, when

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    application = spark.read.format("delta").load(layer_path("silver", "application"))
    previous = spark.read.format("delta").load(layer_path("silver", "previous_application"))

    apps_by_month = application.withColumn("app_month", date_format(col("created_at"), "yyyy-MM"))
    approval = previous.select(
        "SK_ID_CURR",
        when(col("NAME_CONTRACT_STATUS") == "Approved", 1).otherwise(0).alias("approved"),
        spark_abs(col("DAYS_DECISION")).alias("days_to_decision"),  # (unverified) column name — journey/03
    )

    joined = apps_by_month.join(approval, "SK_ID_CURR", "left")
    mart = joined.groupBy("app_month").agg(
        count("*").alias("application_count"),
        (count(when(col("approved") == 1, True)) / count("*") * 100).alias("approval_rate_pct"),
        avg("days_to_decision").alias("avg_days_to_decision"),
    )
    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_loan_funnel"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_loan_funnel"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
