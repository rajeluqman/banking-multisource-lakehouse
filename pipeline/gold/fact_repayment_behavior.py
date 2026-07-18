#!/usr/bin/env python3
"""`fact_repayment_behavior` (ADR-005 Addendum #5, BQ-11/HC-1) — customer-grain behavioral-
risk profile: late-payment/underpayment/DPD signals rolled up from 3 Home Credit child
sources. Grain: one row per `customer_id` (journey/04_DATA_MODEL.md). No SCD — overwrite
snapshot, same stated class as `fact_account_balance` (ADR-005 Add #4).

Fan-out-safe by construction (the load-bearing part of the Add #5 ruling): each Silver
source is aggregated to customer grain (`groupBy(SK_ID_CURR)`) INDEPENDENTLY before any
join — no input to the final join has more than one row per customer, so the join cannot
fan out. This is the same shape that fixed BQ-04's `mart_loan_funnel` fan-out bug, applied
here at Gold-build time in Spark rather than in a serving view. Never join
`sil_installments_payments`/`sil_credit_card_balance`/`sil_pos_cash_balance` to each other
(or to `fact_loan_application`) at raw grain — each is a 1:N-per-customer source.

`SK_ID_CURR` -> `customer_id` resolution via `dim_customer_xwalk` is done HERE (ADR-005
single identity path), same pattern as `fact_loan_application.py`/`fact_previous_
application.py`. `bureau_balance` (the 4th un-Silver'd HC table) is deliberately NOT read —
different join path (`SK_ID_BUREAU`), out of scope for this fact."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    installments = spark.read.format("delta").load(layer_path("silver", "installments_payments"))
    inst_agg = (
        installments
        .withColumn("_days_late", F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT"))
        .withColumn("_is_late", F.col("_days_late") > 0)
        .withColumn("_is_underpaid", F.col("AMT_PAYMENT") < F.col("AMT_INSTALMENT"))
        .groupBy("SK_ID_CURR")
        .agg(
            F.count(F.lit(1)).alias("installment_count"),
            F.avg(F.col("_is_late").cast("double")).alias("late_payment_rate"),
            F.avg(F.col("_is_underpaid").cast("double")).alias("underpayment_rate"),
            F.avg(F.when(F.col("_is_late"), F.col("_days_late"))).alias("avg_days_late"),
        )
    )

    credit_card = spark.read.format("delta").load(layer_path("silver", "credit_card_balance"))
    cc_agg = (
        credit_card
        .withColumn("_utilization", F.col("AMT_BALANCE") / F.col("AMT_CREDIT_LIMIT_ACTUAL"))
        .groupBy("SK_ID_CURR")
        .agg(
            F.sum((F.col("SK_DPD") > 0).cast("int")).alias("cc_months_dpd"),
            F.avg("_utilization").alias("cc_avg_utilization"),
            F.max("SK_DPD").alias("cc_max_dpd"),
        )
    )

    pos_cash = spark.read.format("delta").load(layer_path("silver", "pos_cash_balance"))
    pos_agg = (
        pos_cash
        .groupBy("SK_ID_CURR")
        .agg(
            F.sum((F.col("SK_DPD") > 0).cast("int")).alias("pos_months_dpd"),
            F.max("SK_DPD").alias("pos_max_dpd"),
        )
    )

    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk")) \
        .filter(F.col("source_system") == "home_credit") \
        .select(F.col("native_key").alias("sk_id_curr_str"), "customer_id")

    # inst_agg / cc_agg / pos_agg are each already 1 row per SK_ID_CURR at this point --
    # the outer joins below cannot fan out (see module docstring).
    combined = (
        inst_agg
        .join(cc_agg, "SK_ID_CURR", "outer")
        .join(pos_agg, "SK_ID_CURR", "outer")
        .withColumn(
            "max_dpd",
            F.when(F.col("cc_max_dpd").isNull() & F.col("pos_max_dpd").isNull(), F.lit(None))
            .otherwise(F.greatest(F.coalesce(F.col("cc_max_dpd"), F.lit(0)), F.coalesce(F.col("pos_max_dpd"), F.lit(0)))),
        )
        .withColumn("sk_id_curr_str", F.col("SK_ID_CURR").cast("string"))
    )

    fact = (
        combined.join(xwalk, "sk_id_curr_str", "left")
        .select(
            "customer_id",
            "installment_count", "late_payment_rate", "underpayment_rate", "avg_days_late",
            "cc_months_dpd", "cc_avg_utilization", "pos_months_dpd", "max_dpd",
        )
    )
    fact.write.format("delta").mode("overwrite").save(layer_path("gold", "fact_repayment_behavior"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("fact_repayment_behavior"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
