-- mart_fraud_followup (BQ-03) — % of fraud-hit customers with a CRM follow-up within 48h.
-- Ports pipeline/gold/mart_fraud_followup.py. Grain: single aggregate row (the PySpark
-- source does `groupBy()` with no columns — a whole-population aggregate, not one row per
-- event, preserved as-is here rather than silently "fixed" mid-port).
--
-- ADR-005 Add #4 simplification: fact_crm_case already carries customer_id (xwalk-resolved
-- in the Spark builder), so this join skips dim_customer_xwalk entirely — the original
-- Silver-reading version joined fraud->xwalk->client_id->crm_case; now it's a direct
-- customer_id join, one fewer hop.

{% set sla_hours = 48 %}

with crm_case as (
    select
        customer_id,
        min(try_to_timestamp_ntz(opened_at)) as crm_last_touched
    from {{ source('gold', 'fact_crm_case') }}
    where case_type = 'Fraud Follow-up'
    group by customer_id
),

joined as (
    select
        f.customer_id,
        f.txn_ts,
        c.crm_last_touched,
        (c.crm_last_touched is not null
            and c.crm_last_touched <= dateadd('hour', {{ sla_hours }}, f.txn_ts)
            and c.crm_last_touched >= f.txn_ts) as within_sla
    from {{ source('gold', 'fact_card_fraud') }} f
    left join crm_case c on f.customer_id = c.customer_id
)

select
    count(*) as fraud_event_count,
    count_if(within_sla) as within_sla_count,
    count_if(within_sla) / count(*) * 100 as within_sla_pct
from joined
