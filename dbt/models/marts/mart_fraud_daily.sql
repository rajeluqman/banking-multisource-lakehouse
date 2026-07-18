-- mart_fraud_daily (BQ-02) — fraud txn count & value by date/type, MoM comparable.
-- Grain: one row per (date, transaction_type). Direct port of
-- pipeline/gold/mart_fraud_daily.py — purely Gold-sourced already, no Silver dependency.

select
    cast(txn_ts as date) as txn_date,
    txn_type,
    count(*) as fraud_txn_count,
    sum(amount) as fraud_txn_value
from {{ source('gold', 'fact_card_fraud') }}
group by cast(txn_ts as date), txn_type
