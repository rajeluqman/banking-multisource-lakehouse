#!/usr/bin/env python3
"""`mart_fraud_followup` (BQ-03) — % of fraud-hit customers with a CRM follow-up within 48h.

**Named gap, resolved as a documented proxy (journey/03_DATA_REQUIREMENTS.md BQ-03 row)**:
none of the 5 sources has a real support-ticketing system. Proxy: a Berka CRM `client`
record `updated_at` touch (via drip_feed.py / a real CDC event) within 48h of a fraud event
on that customer stands in for "a follow-up happened." This is NOT a real ticket system —
`journey/08_SERVING_AND_EVIDENCE.md` must describe this mart as partially-simulated, per
the documented gap, not claim a real CRM-ticket SLA.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

from pipeline.common.lake_paths import layer_path

FOLLOWUP_SLA_HOURS = 48


def build(spark: SparkSession) -> None:
    fraud = spark.read.format("delta").load(layer_path("gold", "fact_card_fraud"))
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "berka") \
        .select(col("native_key").alias("client_id"), "customer_id")
    client = spark.read.format("delta").load(layer_path("silver", "client")) \
        .select("client_id", col("updated_at").alias("crm_last_touched"))

    joined = (
        fraud.join(xwalk, "customer_id", "left")
        .join(client, "client_id", "left")
        .withColumn(
            "within_sla",
            when(
                col("crm_last_touched").isNotNull()
                & (col("crm_last_touched") <= col("txn_ts") + dt.timedelta(hours=FOLLOWUP_SLA_HOURS))
                & (col("crm_last_touched") >= col("txn_ts")),
                True,
            ).otherwise(False),
        )
    )
    mart = joined.groupBy().agg(
        count("*").alias("fraud_event_count"),
        count(when(col("within_sla"), True)).alias("within_sla_count"),
    ).withColumn("within_sla_pct", col("within_sla_count") / col("fraud_event_count") * 100)

    mart.write.format("delta").mode("overwrite").save(layer_path("gold", "mart_fraud_followup"))


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_fraud_followup"))
