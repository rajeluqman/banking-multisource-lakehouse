#!/usr/bin/env python3
"""`mart_daily_flows` (BQ-08) — total deposits + daily net flow (in vs out), reported in
MYR (D-12). Grain: one row per date (journey/04_DATA_MODEL.md).

Was NOT actually normalized before this session's fix (real, live bug — BUILD_REPORT.md
§16): `fact_txn.amount` mixes Berka's CZK legs and PaySim's MYR legs, and this mart summed
`amount` directly, silently mixing currencies in `total_in`/`total_out`/`net_flow`. Now
sums `fact_txn.amount_myr` (converted once at the fact layer, `pipeline/gold/fact_txn.py`)
and `total_deposits_snapshot` reads `current_balance_myr` from
`pipeline/gold/common.py::latest_balance_per_account` instead of Berka's raw CZK balance."""

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
        .agg(spark_sum("amount_myr"))
        .withColumnRenamed("in", "total_in")
        .withColumnRenamed("out", "total_out")
    )
    mart = flows.withColumn("net_flow", col("total_in").cast("double") - col("total_out").cast("double"))

    total_deposits = (
        latest_balance_per_account(spark, trans)
        .agg(spark_sum("current_balance_myr").alias("total_deposits"))
        .collect()[0]["total_deposits"]
        or 0.0
    )
    mart = mart.withColumn("total_deposits_snapshot", lit(total_deposits))

    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_daily_flows"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_daily_flows"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
