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
different join path (`SK_ID_BUREAU`), out of scope for this fact.

**Grain integrity (ADR-005 Add #5 ruling: "filter or R-29 -1"):** child `SK_ID_CURR` values
with no `home_credit` xwalk match are the Home Credit TEST-set applicants (only the 307,511
train applicants — the ones carrying `TARGET` — are loaded as `application`/the xwalk; the
~14.8% of child rows keyed to test applicants have no `application` row and no `TARGET`).
Those rows are FILTERED at the end (`customer_id IS NOT NULL`), not written as NULL-keyed
rows — a customer-grain fact must be one-row-per-`customer_id`, and a test-set applicant with
no `TARGET` can never contribute to BQ-11's "does behavior predict default" anyway. The
`-1` unknown-member convention (R-29) does NOT apply here: `customer_id` is this fact's GRAIN,
not a dimension FK, so lumping thousands of distinct unresolved applicants under one `-1` row
would be a meaningless aggregate. Silver-layer orphan quarantine (R-03, `silver_sales.py`)
also removes these upstream, so this filter is a belt-and-suspenders guard."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.common.lake_paths import layer_path


def build(spark: SparkSession) -> None:
    installments = spark.read.format("delta").load(layer_path("silver", "installments_payments"))
    inst_agg = (
        installments
        .withColumn("_days_late", F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT"))
        # An UNPAID installment (NULL DAYS_ENTRY_PAYMENT / AMT_PAYMENT) is delinquent, not
        # "unknown": every installment in this source is past-due (verified 0 rows with
        # DAYS_INSTALMENT >= 0 across the real 13,605,401-row table -- it records the payment
        # history of PRIOR loans, all before the current application). So an installment with
        # no payment recorded is counted as BOTH late and underpaid, giving late_payment_rate/
        # underpayment_rate a denominator of ALL installments (matches journey/05_STTM.md's
        # "fraction of installments" wording). Previously NULLs propagated and silently dropped
        # out of the avg -- the never-paid customer, the STRONGEST delinquency signal, looked
        # clean. avg_days_late stays conditional on a real _days_late (you cannot measure "how
        # late" a never-paid installment is), so it is the mean lateness among paid-late rows.
        .withColumn("_is_late", F.when(F.col("DAYS_ENTRY_PAYMENT").isNull(), F.lit(True))
                    .otherwise(F.col("_days_late") > 0))
        .withColumn("_is_underpaid", F.when(F.col("AMT_PAYMENT").isNull(), F.lit(True))
                    .otherwise(F.col("AMT_PAYMENT") < F.col("AMT_INSTALMENT")))
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
        # try_divide, not a plain `/` -- real AMT_CREDIT_LIMIT_ACTUAL has genuine 0 rows (a
        # zero-limit card snapshot), invisible in the 5000-row local dev-loop sample but real
        # at full Databricks scale (live-caught, ANSI mode raises ArithmeticException on a
        # bare `/` where legacy Spark would have silently returned Infinity/NaN). NULL for a
        # zero limit is correct per the STTM's cc_avg_utilization nullable annotation.
        .withColumn("_utilization_raw", F.try_divide(F.col("AMT_BALANCE"), F.col("AMT_CREDIT_LIMIT_ACTUAL")))
        # Clamp negatives to 0: AMT_BALANCE < 0 is a credit/overpayment balance -> 0 utilization
        # (no risk from utilization), never a negative "utilization" (meaningless as a feature;
        # observed min was -0.085 at full scale). NULL (zero limit) is preserved -- `NULL < 0`
        # is NULL, so it falls through to otherwise and stays NULL, not coerced to 0. Over-limit
        # (>1) is a real risk signal and is deliberately NOT capped.
        .withColumn("_utilization", F.when(F.col("_utilization_raw") < 0, F.lit(0.0))
                    .otherwise(F.col("_utilization_raw")))
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
        # Grain guard (ADR-005 Add #5): drop child SK_ID_CURR with no xwalk match (test-set
        # applicants, no TARGET) so the table is strictly one-row-per-customer_id, as declared
        # in journey/04. See module docstring for why filter, not the R-29 -1 convention.
        .filter(F.col("customer_id").isNotNull())
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
