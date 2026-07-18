-- mart_customer_360 (BQ-01) — product count by type + total relationship value per
-- customer_id, in MYR. Grain: one row per customer_id. Ports pipeline/gold/mart_customer_360.py;
-- has_term_deposit now reads dim_campaign_response (ADR-005 Add #4) instead of Silver
-- campaign_response directly.

with txn_agg as (
    select customer_id, count(*) as txn_count, sum(amount_myr) as txn_value
    from {{ source('gold', 'fact_txn') }}
    group by customer_id
),

loan_agg as (
    select customer_id, count(*) as loan_count
    from {{ source('gold', 'fact_loan_application') }}
    group by customer_id
),

deposit_flag as (
    select
        customer_id,
        iff(subscribed_term_deposit = true, 1, 0) as has_term_deposit
    from {{ source('gold', 'dim_campaign_response') }}
)

select
    c.customer_id,
    coalesce(t.txn_count, 0) as txn_count,
    coalesce(l.loan_count, 0) as loan_count,
    coalesce(d.has_term_deposit, 0) as has_term_deposit,
    coalesce(t.txn_value, 0.0) as total_txn_value,
    (iff(coalesce(l.loan_count, 0) > 0, 1, 0)
        + iff(coalesce(t.txn_count, 0) > 0, 1, 0)
        + coalesce(d.has_term_deposit, 0)) as product_count
from {{ source('gold', 'dim_customer') }} c
left join txn_agg t on c.customer_id = t.customer_id
left join loan_agg l on c.customer_id = l.customer_id
left join deposit_flag d on c.customer_id = d.customer_id
