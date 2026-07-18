-- mart_loan_funnel (BQ-04) — applications/month, approval rate, avg days app->decision.
-- Grain: one row per application month. Ports pipeline/gold/mart_loan_funnel.py.
--
-- ADR-005 Add #4: application_count now reads fact_loan_application directly (already
-- Gold-only, income/target/income-type/created_at all present per staff-DE's ruling — no
-- extension needed); the approval-rate/days-to-decision PROXY reads the new
-- fact_previous_application (the distinct prior-loan population, staff-DE's design). Same
-- "aggregate application at its native grain, then join to the proxy at report grain" shape
-- that fixed the real BQ-04 fan-out bug (journey/08, 2026-07-17) — never re-introduce a
-- 1:N join before counting applications.

with apps_by_month as (
    select
        sk_id_curr,
        to_char(created_at, 'YYYY-MM') as app_month
    from {{ source('gold', 'fact_loan_application') }}
),

application_counts as (
    select app_month, count(*) as application_count
    from apps_by_month
    group by app_month
),

approval_events as (
    select
        m.app_month,
        iff(p.name_contract_status = 'Approved', 1, 0) as approved,
        abs(p.days_decision) as days_to_decision
    from {{ source('gold', 'fact_previous_application') }} p
    inner join apps_by_month m on p.sk_id_curr = m.sk_id_curr
),

approval_agg as (
    select
        app_month,
        count_if(approved = 1) / count(*) * 100 as approval_rate_pct,
        avg(days_to_decision) as avg_days_to_decision
    from approval_events
    group by app_month
)

select
    c.app_month,
    c.application_count,
    a.approval_rate_pct,
    a.avg_days_to_decision
from application_counts c
left join approval_agg a on c.app_month = a.app_month
