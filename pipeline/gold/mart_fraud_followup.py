#!/usr/bin/env python3
"""`mart_fraud_followup` (BQ-03) — % of fraud-hit customers with a CRM follow-up within 48h.

**Real CRM-ticket timestamp (ADR-006 Add #2) — replaces the old `client.updated_at`
proxy.** Salesforce's `Case` object (`sil_crm_case`, filtered to the fraud-follow-up case
type) now supplies a genuine ticket-opened timestamp, closing the CRM-ticket gap named in
journey/03 L8 / journey/06 "known accepted quality gaps". **Still partially-simulated,
disclosed not hidden**: Berka (Case's source) and PaySim (the fraud source) are seeded
independently with no cross-source event timeline, so `seed/salesforce/load_berka.py`'s
synthetic Case `CreatedDate` values are NOT causally linked to any real fraud event — the
SLA metric below is a real mechanism (real object, real timestamp, real join) exercised
against synthetic data, not a claim that any customer's Case was actually opened because of
a specific fraud transaction. `journey/08_SERVING_AND_EVIDENCE.md` must still describe this
mart as partially-simulated.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min as spark_min, when

from pipeline.common.lake_paths import layer_path

FOLLOWUP_SLA_HOURS = 48


def build(spark: SparkSession) -> None:
    fraud = spark.read.format("delta").load(layer_path("gold", "fact_card_fraud"))
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(col("source_system") == "berka") \
        .select(col("native_key").alias("client_id"), "customer_id")
    # A client can have more than one Fraud Follow-up Case — collapse to the EARLIEST per
    # client_id before joining, so a multi-Case client doesn't fan out fraud rows below
    # (fraud is one row per event; this join must stay 1:1 on client_id).
    crm_case = spark.read.format("delta").load(layer_path("silver", "crm_case")) \
        .filter(col("case_type") == "Fraud Follow-up") \
        .groupBy("client_id").agg(spark_min("opened_at").alias("crm_last_touched"))

    joined = (
        fraud.join(xwalk, "customer_id", "left")
        .join(crm_case, "client_id", "left")
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


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_fraud_followup"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
