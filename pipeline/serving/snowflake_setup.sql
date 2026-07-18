-- Snowflake serving-layer bootstrap (Fasa E / ADR-010 / PLAN-dbt-marts-serving-layer.md).
-- Real DDL, not prose — run once against the live Snowflake trial account (ACCOUNTADMIN).
-- Creates: warehouse (finops-approved sizing), resource monitor, the read-only external-table
-- schema over Gold S3 (BANKING.GOLD_EXT, dbt's ONLY source, no physical copy), and the 7
-- Delta Lake-aware external tables dbt's sources.yml reads. dbt's own output schema
-- (ANALYTICS.DBT_MARTS, per ADR-002 addendum) is created separately by the dbt profile, not here.
--
-- Sequenced in two halves around a manual AWS console step (STORAGE_AWS_ROLE_ARN needs an IAM
-- role whose trust policy names Snowflake's generated IAM user + external ID — this repo's Bash
-- tool cannot touch IAM, see PROJECT_STATUS.md 2026-07-18 entry). Run halves in order.

-- ============================================================================
-- HALF 1 — run first. Produces STORAGE_AWS_IAM_USER_ARN / STORAGE_AWS_EXTERNAL_ID for the
-- AWS IAM role (created out-of-band, console) that HALF 2 depends on.
-- ============================================================================

-- @finops sign-off (2026-07-18, PLAN-dbt-marts-serving-layer.md): X-Small, single-cluster,
-- 60s auto-suspend, auto-resume on, 300s statement timeout, resource-monitor-gated.
CREATE WAREHOUSE IF NOT EXISTS DBT_WH
  WAREHOUSE_SIZE = 'XSMALL'
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 300
  COMMENT = 'dbt-on-Snowflake marts warehouse (PLAN-dbt-marts-serving-layer.md, finops-approved 2026-07-18)';

-- Hard backstop on cumulative trial-credit exposure, not just per-query cost (finops requirement).
CREATE RESOURCE MONITOR IF NOT EXISTS DBT_MARTS_MONITOR
  WITH CREDIT_QUOTA = 5
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE DBT_WH SET RESOURCE_MONITOR = DBT_MARTS_MONITOR;

-- Read-only external-table home. Deliberately a separate database from ANALYTICS.DBT_MARTS
-- (dbt's output schema, created by the dbt profile) — this schema is the read-only mirror of
-- S3 Gold truth; ANALYTICS.DBT_MARTS is the derived analytics-engineering layer on top of it.
CREATE DATABASE IF NOT EXISTS BANKING;
CREATE SCHEMA IF NOT EXISTS BANKING.GOLD_EXT
  COMMENT = 'Read-only Snowflake external tables over s3://banking-lakehouse-pipeline/banking/gold/. NO physical copy (ADR-002 addendum, PLAN-dbt-marts-serving-layer.md Q2) — S3 stays sole physical truth.';

USE DATABASE BANKING;
USE SCHEMA GOLD_EXT;

-- STORAGE_AWS_ROLE_ARN references a role that does not exist yet at CREATE time — Snowflake
-- does not validate the ARN until something tries to actually assume it (CREATE STAGE / a
-- REFRESH). The role must be named exactly this, created via the AWS console (HALF 1.5 below).
CREATE STORAGE INTEGRATION IF NOT EXISTS BANKING_GOLD_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::579880301047:role/snowflake-role-banking-lakehouse-gold-ro'
  STORAGE_ALLOWED_LOCATIONS = ('s3://banking-lakehouse-pipeline/banking/gold/');

-- Run this and hand STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID to the AWS console step.
DESC STORAGE INTEGRATION BANKING_GOLD_S3_INT;

-- ============================================================================
-- HALF 1.5 — MANUAL, out-of-band, AWS console (this Bash tool is blocked from IAM entirely,
-- even read-only). Create IAM role `snowflake-role-banking-lakehouse-gold-ro` in account
-- 579880301047 with:
--   Trust policy: Principal = the STORAGE_AWS_IAM_USER_ARN from DESC STORAGE INTEGRATION above,
--     Condition sts:ExternalId = the STORAGE_AWS_EXTERNAL_ID from the same output.
--   Permissions policy: s3:GetObject on arn:aws:s3:::banking-lakehouse-pipeline/banking/gold/*
--     and s3:ListBucket on arn:aws:s3:::banking-lakehouse-pipeline with a
--     s3:prefix=banking/gold/* condition (least-privilege, matches STORAGE_ALLOWED_LOCATIONS).
-- ============================================================================

-- ============================================================================
-- HALF 2 — run after the IAM role above exists. Creates the stage + 7 Delta-aware external
-- tables. Explicit column lists are required (Snowflake does not infer from the Delta schema) —
-- these were extracted from the REAL physical _delta_log/00000000000000000000.json schemaString
-- on S3 (ground truth, not the PySpark source) for all 7 tables, 2026-07-18.
--
-- Live-tested finding (2026-07-18, not in the docs example that suggested it): for
-- TABLE_FORMAT = DELTA, `PARTITION_TYPE = USER_SPECIFIED` is REJECTED outright ("invalid
-- property combination"), and a `PARTITION BY (...)` clause with partition columns extracted
-- via `metadata$external_table_partition` or even a plain `value:col::type` GET expression is
-- REJECTED too ("GET is not supported in an external table partition column expression").
-- Resolution: omit PARTITION_TYPE and PARTITION BY entirely — Delta's own transaction-log
-- partitionValues already give Snowflake what it needs; txn_year/txn_month are declared as
-- ordinary `value:col::type` columns like everything else and still resolve correctly (verified:
-- fact_txn read back exactly 6,363,370 rows, the known-correct baseline, txn_year populated).
-- ============================================================================

USE DATABASE BANKING;
USE SCHEMA GOLD_EXT;

CREATE STAGE IF NOT EXISTS BANKING_GOLD_STAGE
  URL = 's3://banking-lakehouse-pipeline/banking/gold/'
  STORAGE_INTEGRATION = BANKING_GOLD_S3_INT;

CREATE OR REPLACE EXTERNAL TABLE fact_txn (
    txn_id         STRING    AS (value:txn_id::string),
    customer_id    STRING    AS (value:customer_id::string),
    txn_ts         TIMESTAMP_NTZ AS (value:txn_ts::timestamp_ntz),
    txn_type       STRING    AS (value:txn_type::string),
    amount         DOUBLE    AS (value:amount::double),
    currency       STRING    AS (value:currency::string),
    is_fraud       BOOLEAN   AS (value:is_fraud::boolean),
    source_system  STRING    AS (value:source_system::string),
    amount_myr     DOUBLE    AS (value:amount_myr::double),
    txn_year       INTEGER   AS (value:txn_year::integer),
    txn_month      INTEGER   AS (value:txn_month::integer)
)
LOCATION = @BANKING_GOLD_STAGE/fact_txn/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/fact_txn (fact grain: 1 row/txn). Refresh via ALTER EXTERNAL TABLE ... REFRESH after each Gold rebuild.';

CREATE OR REPLACE EXTERNAL TABLE fact_card_fraud (
    txn_id         STRING    AS (value:txn_id::string),
    customer_id    STRING    AS (value:customer_id::string),
    txn_ts         TIMESTAMP_NTZ AS (value:txn_ts::timestamp_ntz),
    txn_type       STRING    AS (value:txn_type::string),
    amount         DOUBLE    AS (value:amount::double),
    currency       STRING    AS (value:currency::string),
    amount_myr     DOUBLE    AS (value:amount_myr::double),
    txn_year       INTEGER   AS (value:txn_year::integer),
    txn_month      INTEGER   AS (value:txn_month::integer)
)
LOCATION = @BANKING_GOLD_STAGE/fact_card_fraud/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/fact_card_fraud (PaySim isFraud=1 subset).';

CREATE OR REPLACE EXTERNAL TABLE fact_loan_application (
    customer_id       STRING  AS (value:customer_id::string),
    sk_id_curr        STRING  AS (value:SK_ID_CURR::string),
    target             NUMBER  AS (value:TARGET::number),
    amt_income_total  DOUBLE  AS (value:AMT_INCOME_TOTAL::double),
    name_income_type  STRING  AS (value:NAME_INCOME_TYPE::string),
    created_at        DATE    AS (value:created_at::date)
)
LOCATION = @BANKING_GOLD_STAGE/fact_loan_application/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/fact_loan_application (Home Credit application grain).';

CREATE OR REPLACE EXTERNAL TABLE dim_customer (
    customer_id             STRING AS (value:customer_id::string),
    source_priority_rank    STRING AS (value:source_priority_rank::string),
    birth_date              STRING AS (value:birth_date::string),
    gender                  STRING AS (value:gender::string),
    district_id             STRING AS (value:district_id::string)
)
LOCATION = @BANKING_GOLD_STAGE/dim_customer/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/dim_customer (golden record, Type 1 SCD).';

CREATE OR REPLACE EXTERNAL TABLE dim_customer_xwalk (
    customer_id             STRING AS (value:customer_id::string),
    source_system           STRING AS (value:source_system::string),
    native_key              STRING AS (value:native_key::string),
    source_priority_rank    STRING AS (value:source_priority_rank::string)
)
LOCATION = @BANKING_GOLD_STAGE/dim_customer_xwalk/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/dim_customer_xwalk (MDM crosswalk, grain: customer_id x source_system).';

CREATE OR REPLACE EXTERNAL TABLE dim_date (
    date_key        DATE    AS (value:date_key::date),
    year            INTEGER AS (value:year::integer),
    month           INTEGER AS (value:month::integer),
    day             INTEGER AS (value:day::integer),
    month_name      STRING  AS (value:month_name::string),
    day_of_week     STRING  AS (value:day_of_week::string)
)
LOCATION = @BANKING_GOLD_STAGE/dim_date/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/dim_date (calendar dimension).';

CREATE OR REPLACE EXTERNAL TABLE dim_fx_rate (
    currency_code   STRING AS (value:currency_code::string),
    rate_to_myr     DOUBLE AS (value:rate_to_myr::double),
    rate_as_of      STRING AS (value:rate_as_of::string),
    note            STRING AS (value:note::string)
)
LOCATION = @BANKING_GOLD_STAGE/dim_fx_rate/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/dim_fx_rate (D-12 static FX seed, grain: currency_code).';

-- Delta external tables need an explicit REFRESH after creation and after every Gold rebuild
-- (AUTO_REFRESH is mechanically disallowed for TABLE_FORMAT=DELTA — Snowflake docs, 2026-07-18).
ALTER EXTERNAL TABLE fact_txn REFRESH;
ALTER EXTERNAL TABLE fact_card_fraud REFRESH;
ALTER EXTERNAL TABLE fact_loan_application REFRESH;
ALTER EXTERNAL TABLE dim_customer REFRESH;
ALTER EXTERNAL TABLE dim_customer_xwalk REFRESH;
ALTER EXTERNAL TABLE dim_date REFRESH;
ALTER EXTERNAL TABLE dim_fx_rate REFRESH;

-- ============================================================================
-- HALF 3 (2026-07-18, ADR-005 Addendum #4) — the 5 new external tables over the Silver->Gold
-- promotions (dim_campaign_response, fact_crm_case, fact_previous_application,
-- fact_account_balance, bridge_customer_account). Column types pulled from the REAL deployed
-- S3 _delta_log/00000000000000000000.json schemaString AFTER the Databricks deploy (ground
-- truth) — NOT the local dev-loop fixture, which was caught carrying a stale pre-boolean-cast
-- schema for campaign_response (credit_in_default/subscribed_term_deposit as string there vs
-- boolean at real canonical scale — anti-shortcut protocol catch, 2026-07-18).
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE dim_campaign_response (
    customer_id               STRING  AS (value:customer_id::string),
    job                       STRING  AS (value:job::string),
    marital                   STRING  AS (value:marital::string),
    education                 STRING  AS (value:education::string),
    credit_in_default         BOOLEAN AS (value:credit_in_default::boolean),
    avg_yearly_balance        BIGINT  AS (value:avg_yearly_balance::bigint),
    prior_campaign_outcome    STRING  AS (value:prior_campaign_outcome::string),
    subscribed_term_deposit   BOOLEAN AS (value:subscribed_term_deposit::boolean)
)
LOCATION = @BANKING_GOLD_STAGE/dim_campaign_response/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/dim_campaign_response (ADR-005 Add #4, grain: customer_id). credit_in_default/job/education stay confidential/risk-classified per journey/09 — scope grants to BQ-05/06-facing roles only.';

CREATE OR REPLACE EXTERNAL TABLE fact_crm_case (
    case_id        STRING AS (value:case_id::string),
    customer_id    STRING AS (value:customer_id::string),
    case_type      STRING AS (value:case_type::string),
    opened_at      STRING AS (value:opened_at::string)
)
LOCATION = @BANKING_GOLD_STAGE/fact_crm_case/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/fact_crm_case (ADR-005 Add #4, grain: case_id).';

CREATE OR REPLACE EXTERNAL TABLE fact_previous_application (
    sk_id_prev             BIGINT AS (value:sk_id_prev::bigint),
    customer_id            STRING AS (value:customer_id::string),
    sk_id_curr             BIGINT AS (value:sk_id_curr::bigint),
    name_contract_status   STRING AS (value:name_contract_status::string),
    days_decision          BIGINT AS (value:days_decision::bigint)
)
LOCATION = @BANKING_GOLD_STAGE/fact_previous_application/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/fact_previous_application (ADR-005 Add #4, grain: sk_id_prev).';

CREATE OR REPLACE EXTERNAL TABLE fact_account_balance (
    account_id             STRING AS (value:account_id::string),
    current_balance        DOUBLE AS (value:current_balance::double),
    current_balance_myr    DOUBLE AS (value:current_balance_myr::double),
    currency               STRING AS (value:currency::string)
)
LOCATION = @BANKING_GOLD_STAGE/fact_account_balance/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/fact_account_balance (ADR-005 Add #4, grain: account_id, current-balance snapshot).';

CREATE OR REPLACE EXTERNAL TABLE bridge_customer_account (
    customer_id     STRING AS (value:customer_id::string),
    account_id      STRING AS (value:account_id::string),
    relation_type   STRING AS (value:relation_type::string)
)
LOCATION = @BANKING_GOLD_STAGE/bridge_customer_account/
FILE_FORMAT = (TYPE = PARQUET)
TABLE_FORMAT = DELTA
REFRESH_ON_CREATE = FALSE
AUTO_REFRESH = FALSE
COMMENT = 'Delta external table over banking/gold/bridge_customer_account (ADR-005 Add #4, grain: customer_id x account_id N:N bridge, not a CTE).';

ALTER EXTERNAL TABLE dim_campaign_response REFRESH;
ALTER EXTERNAL TABLE fact_crm_case REFRESH;
ALTER EXTERNAL TABLE fact_previous_application REFRESH;
ALTER EXTERNAL TABLE fact_account_balance REFRESH;
ALTER EXTERNAL TABLE bridge_customer_account REFRESH;

-- serving_ro role (journey/09_SECURITY_AND_ACCESS.md line 69) — Gold-only, read-only, Snowflake side.
CREATE ROLE IF NOT EXISTS SERVING_RO;
GRANT USAGE ON DATABASE BANKING TO ROLE SERVING_RO;
GRANT USAGE ON SCHEMA BANKING.GOLD_EXT TO ROLE SERVING_RO;
GRANT SELECT ON ALL TABLES IN SCHEMA BANKING.GOLD_EXT TO ROLE SERVING_RO;
GRANT USAGE ON WAREHOUSE DBT_WH TO ROLE SERVING_RO;
