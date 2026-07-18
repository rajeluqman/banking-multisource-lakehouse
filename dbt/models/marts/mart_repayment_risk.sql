-- mart_repayment_risk (BQ-11/HC-1) — repayment discipline vs default. Grain: one row per
-- customer_id. Extends BQ-05's demographic risk view with a behavioral signal.
--
-- Both sources are ALREADY customer-grain (fact_repayment_behavior: fan-out-safe Spark
-- aggregation, ADR-005 Add #5; fact_loan_application: one row per SK_ID_CURR/customer_id) —
-- this is a 1:1 join, cannot fan out. Reads Gold ONLY (journey/09 serving_ro = Gold-only).
--
-- Selection scope (stated, not incidental): this is an INNER join, so the mart covers only
-- customers present in BOTH facts — i.e. train-set applicants (fact_loan_application, which
-- carries TARGET) who ALSO have prior-loan child records (fact_repayment_behavior). Applicants
-- with no prior-loan history are excluded (no behavioral signal to correlate), and test-set
-- applicants were already filtered out of fact_repayment_behavior upstream (no TARGET). Net:
-- BQ-11 answers "does repayment behavior predict default" for established borrowers with a prior
-- loan history — the population where the question is actually answerable. This is the correct
-- scope for the question, but it is a scope, so it is named here rather than left implicit.

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
