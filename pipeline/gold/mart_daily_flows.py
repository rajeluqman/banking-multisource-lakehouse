#!/usr/bin/env python3
"""`mart_daily_flows` (BQ-08) — total deposits + daily net flow (in vs out). Grain: one row
per date (journey/04_DATA_MODEL.md). Currency already normalized to MYR at Silver (D-12)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, to_date, when

from pipeline.common.lake_paths import layer_path
from pipeline.gold.common import latest_balance_per_account

# "type" in Berka trans / txn_type in PaySim carries the same in/out signal for its rows.
INFLOW_TYPES = ["CASH_IN", "DEPOSIT", "PRIJEM"]


def build(spark: SparkSession) -> None:
    fact_txn = spark.read.format("delta").load(layer_path("gold", "fact_txn"))
    trans = spark.read.format("delta").load(layer_path("silver", "trans"))

    flows = (
        fact_txn.withColumn("txn_date", to_date(col("txn_ts")))
        .withColumn("direction", when(col("txn_type").isin(*INFLOW_TYPES), lit("in")).otherwise(lit("out")))
        .groupBy("txn_date")
        .pivot("direction", ["in", "out"])
        .agg(spark_sum("amount"))
        .withColumnRenamed("in", "total_in")
        .withColumnRenamed("out", "total_out")
    )
    mart = flows.withColumn("net_flow", col("total_in").cast("double") - col("total_out").cast("double"))

    total_deposits = (
        latest_balance_per_account(trans)
        .agg(spark_sum("current_balance").alias("total_deposits"))
        .collect()[0]["total_deposits"]
        or 0.0
    )
    mart = mart.withColumn("total_deposits_snapshot", lit(total_deposits))

    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_daily_flows"))


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_daily_flows"))
