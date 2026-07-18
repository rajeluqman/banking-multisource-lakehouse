-- mart_risk_segment (BQ-05) — default rate by segment, current high-risk ACTIVE customers.
-- Grain: one row per (customer_id, segment). Ports pipeline/gold/mart_risk_segment.py.
--
-- ADR-005 Add #4 simplification: fact_loan_application already carries customer_id directly
-- (no xwalk hop needed — the old Silver `application` read required joining SK_ID_CURR
-- through dim_customer_xwalk to get customer_id; the promoted fact already has it), and
-- dim_campaign_response already carries customer_id too, so this becomes a clean
-- customer_id join throughout instead of a cast-string SK_ID_CURR join.

with income_bounds as (
    select
        approx_percentile(amt_income_total, 0.25) as p25,
        approx_percentile(amt_income_total, 0.75) as p75
    from {{ source('gold', 'fact_loan_application') }}
),

segmented as (
    select
        l.customer_id,
        l.target,
        l.name_income_type,
        case
            when l.amt_income_total < b.p25 then 'LOW'
            when l.amt_income_total > b.p75 then 'HIGH'
            else 'MEDIUM'
        end as income_band
    from {{ source('gold', 'fact_loan_application') }} l
    cross join income_bounds b
),

last_activity as (
    select customer_id, max(txn_ts) as last_txn_ts
    from {{ source('gold', 'fact_txn') }}
    group by customer_id
),

joined as (
    select
        s.customer_id,
        s.income_band,
        s.name_income_type,
        s.target,
        (s.target is not null and c.credit_in_default is not null
            and s.target::int != iff(c.credit_in_default, 1, 0)) as default_disagreement
    from segmented s
    left join {{ source('gold', 'dim_campaign_response') }} c on s.customer_id = c.customer_id
    left join last_activity a on s.customer_id = a.customer_id
)

select
    customer_id,
    income_band,
    name_income_type,
    count_if(target = 1) as is_default,
    count_if(default_disagreement) as default_signal_disagreement
from joined
group by customer_id, income_band, name_income_type
