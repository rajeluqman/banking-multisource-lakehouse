-- mart_repayment_risk (BQ-11/HC-1) — repayment discipline vs default. Grain: one row per
-- customer_id. Extends BQ-05's demographic risk view with a behavioral signal.
--
-- Both sources are ALREADY customer-grain (fact_repayment_behavior: fan-out-safe Spark
-- aggregation, ADR-005 Add #5; fact_loan_application: one row per SK_ID_CURR/customer_id) —
-- this is a 1:1 join, cannot fan out. Reads Gold ONLY (journey/09 serving_ro = Gold-only).
--
-- fact_repayment_behavior has ~14% NULL customer_id rows by design (child SK_ID_CURR with no
-- home_credit xwalk match, R-29 late-arriving pattern, verified 2026-07-18 at full canonical
-- scale: 47,180 of 334,710 rows) — inner join to fact_loan_application naturally drops those,
-- since a NULL customer_id can never match a real application's customer_id either.

select
    r.customer_id,
    l.target as target_default,
    r.installment_count,
    r.late_payment_rate,
    r.underpayment_rate,
    r.avg_days_late,
    r.cc_months_dpd,
    r.cc_avg_utilization,
    r.pos_months_dpd,
    r.max_dpd
from {{ source('gold', 'fact_repayment_behavior') }} r
inner join {{ source('gold', 'fact_loan_application') }} l on r.customer_id = l.customer_id
