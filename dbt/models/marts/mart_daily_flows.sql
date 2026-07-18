-- mart_daily_flows (BQ-08) — total deposits + daily net flow (in vs out), in MYR.
-- Grain: one row per date. Ports pipeline/gold/mart_daily_flows.py; total_deposits_snapshot
-- now reads fact_account_balance (ADR-005 Add #4) instead of Silver `trans` directly.

{% set inflow_types = ['CASH_IN', 'DEPOSIT', 'PRIJEM'] %}

with flows as (
    select
        cast(txn_ts as date) as txn_date,
        sum(iff(txn_type in ({{ "'" ~ inflow_types|join("','") ~ "'" }}), amount_myr, 0)) as total_in,
        sum(iff(txn_type in ({{ "'" ~ inflow_types|join("','") ~ "'" }}), 0, amount_myr)) as total_out
    from {{ source('gold', 'fact_txn') }}
    group by cast(txn_ts as date)
),

total_deposits as (
    select coalesce(sum(current_balance_myr), 0.0) as total_deposits
    from {{ source('gold', 'fact_account_balance') }}
)

select
    f.txn_date,
    f.total_in,
    f.total_out,
    f.total_in - f.total_out as net_flow,
    d.total_deposits as total_deposits_snapshot
from flows f
cross join total_deposits d
