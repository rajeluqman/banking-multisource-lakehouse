#!/usr/bin/env python3
"""`mart_fraud_daily` (BQ-02) — fraud txn count & value by date/type, MoM comparable.
Grain: one row per (date, transaction_type) (journey/04_DATA_MODEL.md)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, to_date

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    fraud = spark.read.format("delta").load(layer_path("gold", "fact_card_fraud"))
    mart = (
        fraud.withColumn("txn_date", to_date(col("txn_ts")))
        .groupBy("txn_date", "txn_type")
        .agg(count("*").alias("fraud_txn_count"), spark_sum("amount").alias("fraud_txn_value"))
    )
    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_fraud_daily"))


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_fraud_daily"))
