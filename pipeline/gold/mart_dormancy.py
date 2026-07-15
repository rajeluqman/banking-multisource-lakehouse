#!/usr/bin/env python3
"""`mart_dormancy` (BQ-07) — customers with no txn in 90 days this month (journey/03).
Grain: one row per dormant customer_id per month."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, date_format

from pipeline.common.lake_paths import layer_path

DORMANCY_WINDOW_DAYS = 90  # journey/03_DATA_REQUIREMENTS.md — same window as BQ-05/06


def build(spark: SparkSession) -> None:
    dim_customer = spark.read.format("delta").load(layer_path("gold", "dim_customer"))
    fact_txn = spark.read.format("delta").load(layer_path("gold", "fact_txn"))

    last_activity = fact_txn.groupBy("customer_id").agg({"txn_ts": "max"}).withColumnRenamed("max(txn_ts)", "last_txn_ts")
    dormant = (
        dim_customer.select("customer_id")
        .join(last_activity, "customer_id", "left")
        .withColumn("days_since_last_txn", datediff(current_date(), col("last_txn_ts").cast("date")))
        .filter(col("last_txn_ts").isNull() | (col("days_since_last_txn") >= DORMANCY_WINDOW_DAYS))
        .withColumn("as_of_month", date_format(current_date(), "yyyy-MM"))
    )
    mart = dormant.select("customer_id", "as_of_month", "last_txn_ts", "days_since_last_txn")
    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_dormancy"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_dormancy"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
