-- mart_cross_sell (BQ-06) — healthy-deposit, active, no-card/loan customers, ranked by prior
-- campaign responsiveness. Grain: one row per qualifying customer_id (preserved as-authored —
-- the source PySpark does not de-duplicate a customer holding multiple accounts; a multi-
-- account customer can fan out here same as it did before this port, not a new behavior).
-- Ports pipeline/gold/mart_cross_sell.py — balances/bridge/campaign-response all now read
-- from the ADR-005 Add #4 Gold promotions instead of Silver disp/trans/campaign_response.

with balance_p50 as (
    select approx_percentile(current_balance_myr, 0.5) as p50
    from {{ source('gold', 'fact_account_balance') }}
),

deposits as (
    select
        b.customer_id,
        fab.current_balance_myr as current_balance
    from {{ source('gold', 'bridge_customer_account') }} b
    left join {{ source('gold', 'fact_account_balance') }} fab on b.account_id = fab.account_id
),

has_loan as (
    select distinct customer_id, true as has_loan
    from {{ source('gold', 'fact_loan_application') }}
),

has_card as (
    select distinct customer_id, true as has_card
    from {{ source('gold', 'fact_txn') }}
    where source_system = 'paysim'
),

last_activity as (
    select customer_id, max(txn_ts) as last_txn_ts
    from {{ source('gold', 'fact_txn') }}
    group by customer_id
)

select
    c.customer_id,
    d.current_balance,
    a.last_txn_ts,
    cr.prior_campaign_outcome,
    cr.subscribed_term_deposit
from {{ source('gold', 'dim_customer') }} c
left join deposits d on c.customer_id = d.customer_id
left join has_loan hl on c.customer_id = hl.customer_id
left join has_card hc on c.customer_id = hc.customer_id
left join last_activity a on c.customer_id = a.customer_id
left join {{ source('gold', 'dim_campaign_response') }} cr on c.customer_id = cr.customer_id
cross join balance_p50 bp
where hl.has_loan is null
  and hc.has_card is null
  and d.current_balance >= bp.p50
