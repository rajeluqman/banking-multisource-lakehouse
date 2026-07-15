#!/usr/bin/env python3
"""`fact_loan_application` — one row per Home Credit application (journey/04). Feeds
BQ-04 (loan funnel) and BQ-05 (default risk)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "home_credit") \
        .select(col("native_key").alias("sk_id_curr"), "customer_id")
    application = spark.read.format("delta").load(layer_path("silver", "application")) \
        .withColumn("sk_id_curr", col("SK_ID_CURR").cast("string"))

    fact = (
        application.join(xwalk, "sk_id_curr", "left")
        .select("customer_id", "SK_ID_CURR", "TARGET", "AMT_INCOME_TOTAL", "NAME_INCOME_TYPE",
                "created_at")
    )
    fact.write.format("delta").mode("append").save(layer_path("gold", "fact_loan_application"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_loan_application"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
