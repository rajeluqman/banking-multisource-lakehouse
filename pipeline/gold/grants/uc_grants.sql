-- Unity Catalog RBAC grants (D-16 §3, R-31, journey/09_SECURITY_AND_ACCESS.md).
-- REAL GRANTs, not prose — this file IS the enforcement, run once against the canonical
-- Databricks/UC workspace (D-01 Add #3). Load-bearing rule: raw layers (Landing/Bronze)
-- hold unmasked PII (R-27) — no analyst/serving role may read them.

-- pipeline_svc: the extractors/transforms service identity (not a human) — full pipeline access.
GRANT USE CATALOG ON CATALOG banking TO `pipeline_svc`;
GRANT ALL PRIVILEGES ON SCHEMA banking.landing TO `pipeline_svc`;
GRANT ALL PRIVILEGES ON SCHEMA banking.bronze TO `pipeline_svc`;
GRANT ALL PRIVILEGES ON SCHEMA banking.silver TO `pipeline_svc`;
GRANT ALL PRIVILEGES ON SCHEMA banking.gold TO `pipeline_svc`;

-- data_engineer: development/debugging — Silver/Gold write, Bronze read-only, NO Landing.
GRANT USE CATALOG ON CATALOG banking TO `data_engineer`;
GRANT SELECT ON SCHEMA banking.bronze TO `data_engineer`;
GRANT SELECT, MODIFY ON SCHEMA banking.silver TO `data_engineer`;
GRANT SELECT, MODIFY ON SCHEMA banking.gold TO `data_engineer`;

-- analyst_marketing: Gold-only, masked marts (BQ-01/06/09) — NEVER Landing/Bronze/Silver raw PII.
GRANT USE CATALOG ON CATALOG banking TO `analyst_marketing`;
GRANT SELECT ON TABLE banking.gold.mart_customer_360 TO `analyst_marketing`;
GRANT SELECT ON TABLE banking.gold.mart_cross_sell TO `analyst_marketing`;
GRANT SELECT ON TABLE banking.gold.fact_txn TO `analyst_marketing`;
GRANT SELECT ON TABLE banking.gold.dim_customer TO `analyst_marketing`;

-- fraud_ops: Gold fraud marts + isFraud (BQ-02/03).
GRANT USE CATALOG ON CATALOG banking TO `fraud_ops`;
GRANT SELECT ON TABLE banking.gold.mart_fraud_daily TO `fraud_ops`;
GRANT SELECT ON TABLE banking.gold.mart_fraud_followup TO `fraud_ops`;
GRANT SELECT ON TABLE banking.gold.fact_card_fraud TO `fraud_ops`;

-- risk: Gold risk marts (BQ-05).
GRANT USE CATALOG ON CATALOG banking TO `risk`;
GRANT SELECT ON TABLE banking.gold.mart_risk_segment TO `risk`;

-- serving_ro (Snowflake external tables, Fasa E): Gold-only, read-only.
GRANT USE CATALOG ON CATALOG banking TO `serving_ro`;
GRANT SELECT ON SCHEMA banking.gold TO `serving_ro`;

-- landing_admin: break-glass only, audited — raw Landing/Bronze PII access.
GRANT USE CATALOG ON CATALOG banking TO `landing_admin`;
GRANT SELECT ON SCHEMA banking.landing TO `landing_admin`;
GRANT SELECT ON SCHEMA banking.bronze TO `landing_admin`;

-- Explicit denial documentation (UC denies-by-default; these REVOKEs are defense-in-depth,
-- making the R-31 rule literal even if a future grant is added carelessly to one of these roles).
REVOKE ALL PRIVILEGES ON SCHEMA banking.landing FROM `analyst_marketing`, `fraud_ops`, `risk`, `serving_ro`;
REVOKE ALL PRIVILEGES ON SCHEMA banking.bronze FROM `analyst_marketing`, `fraud_ops`, `risk`, `serving_ro`;
