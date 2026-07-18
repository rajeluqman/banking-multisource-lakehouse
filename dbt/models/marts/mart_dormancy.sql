-- mart_dormancy (BQ-07) — customers with no txn in 90 days this month. Grain: one row per
-- dormant customer_id per month. Direct port of pipeline/gold/mart_dormancy.py — purely
-- Gold-sourced already, no Silver dependency.

{% set dormancy_window_days = 90 %}

with last_activity as (
    select customer_id, max(txn_ts) as last_txn_ts
    from {{ source('gold', 'fact_txn') }}
    group by customer_id
)

select
    c.customer_id,
    to_char(current_date(), 'YYYY-MM') as as_of_month,
    a.last_txn_ts,
    datediff('day', cast(a.last_txn_ts as date), current_date()) as days_since_last_txn
from {{ source('gold', 'dim_customer') }} c
left join last_activity a on c.customer_id = a.customer_id
where a.last_txn_ts is null
   or datediff('day', cast(a.last_txn_ts as date), current_date()) >= {{ dormancy_window_days }}
