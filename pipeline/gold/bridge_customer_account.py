#!/usr/bin/env python3
"""`bridge_customer_account` (ADR-005 Addendum #4) — the customer<->account N:N as a proper
BRIDGE table (not a CTE — journey/04_DATA_MODEL.md lines 43-46 name this pattern explicitly),
promoted to Gold so the dbt-on-Snowflake serving layer reads Gold ONLY, never Silver `disp`.
Grain: one row per (`customer_id`, `account_id`) (journey/04_DATA_MODEL.md). Type 1 overwrite.

Berka's `disp` encodes the relationship: a jointly-held account has an OWNER row AND a
DISPONENT row, so this is genuinely N:N (a customer can hold many accounts; an account can be
held by more than one customer). `relation_type` (OWNER/DISPONENT) is preserved — BOTH types are
retained here; a consuming mart decides whether to filter (e.g. `fact_txn`'s Berka leg restricts
to OWNER to preserve transaction grain, but `mart_cross_sell` reads all disp rows). Keeping the
type in the bridge lets each consumer choose, rather than baking one filter into the bridge.

The Berka `client_id`->`customer_id` resolution (via `dim_customer_xwalk`) is done HERE in the
Spark builder (ADR-005 single identity path), so dbt never touches the crosswalk or Silver."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "berka") \
        .select(col("native_key").alias("client_id"), "customer_id")
    disp = spark.read.format("delta").load(layer_path("silver", "disp"))

    bridge = (
        disp.join(xwalk, "client_id", "left")
        .select("customer_id", "account_id", col("type").alias("relation_type"))
    )
    bridge.write.format("delta").mode("overwrite").save(layer_path("gold", "bridge_customer_account"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("bridge_customer_account"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
