# 04 — Data Model

## Modelling approach chosen (and why)
**Star schema** for Gold, with one keystone conformed dimension (`dim_customer_xwalk`) resolving
identity across four sources that share no native key. Full rationale + alternatives considered:
`governance/ADR/ADR-005-star-schema-gold-and-mdm-xwalk.md`. Silver is 1-row-per-latest-state-per-
entity (MERGE upsert target, not a star — star-shaping happens at Gold).

## Grain declarations (one row per table/model)
| Table/model | Grain | Business entity | Layer |
|---|---|---|---|
| seed/build_xwalk.py output → Silver `dim_customer_xwalk` | one row per (bank-wide customer_id, source system) pair | Customer identity resolution | Silver (built at seed, re-synced at Silver) |
| `sil_application` | one row per `SK_ID_CURR` | Loan applicant snapshot | Silver |
| `sil_bureau` | one row per bureau-reported credit line | External credit bureau record | Silver |
| `sil_previous_application` | one row per prior loan application | Prior application | Silver |
| `sil_card_txn` | one row per PaySim transaction | Card/mobile-money transaction | Silver |
| `sil_client` / `sil_account` / `sil_disp` / `sil_trans` / `sil_district` / `sil_crm_case` | one row per Berka source-table record (native Berka grain preserved) | CRM customer/account/product (sourced from Salesforce Contact/Account/AccountContactRelation/Transaction__c/District__c/Case, ADR-006 Add #2 — `card`/`loan` intentionally not carried into this build, build-scope note, neither read by any Gold builder) | Silver |
| `sil_crm_case` | one row per Salesforce Case (CRM ticket) | CRM support / fraud-follow-up ticket (ADR-006 Add #2 — resolves BQ-03's journey/03 L8 gap) | Silver |
| `sil_obp_accounts` / `sil_obp_transactions` | one row per OBP account / transaction | Core banking account/transaction | Silver |
| `sil_campaign_response` | one row per Teradata Bank Marketing record (post-xwalk-linkage) | Marketing/campaign response (ADR-006) | Silver |
| `dim_customer` | one row per bank-wide `customer_id` (golden record) | Customer | Gold |
| `dim_date` | one row per calendar date | Date | Gold |
| `dim_fx_rate` | one row per `currency_code` | FX reference rate (D-12, ADR-005 addendum #1, this session) | Gold |
| `fact_txn` | one row per transaction (any source, unioned + conformed) | Financial transaction | Gold |
| `fact_card_fraud` | one row per PaySim transaction flagged `isFraud=1` | Fraud event | Gold |
| `fact_loan_application` | one row per Home Credit application | Loan application | Gold |
| `mart_customer_360` (BQ-01) | one row per customer_id | Customer relationship summary | Gold mart |
| `mart_fraud_daily` (BQ-02) | one row per (date, transaction_type) | Fraud trend | Gold mart |
| `mart_fraud_followup` (BQ-03) | one row per fraud event | Fraud SLA | Gold mart |
| `mart_loan_funnel` (BQ-04) | one row per (application month) | Loan funnel | Gold mart |
| `mart_risk_segment` (BQ-05) | one row per (customer_id, segment) | Risk | Gold mart |
| `mart_cross_sell` (BQ-06) | one row per qualifying customer_id | Cross-sell target | Gold mart |
| `mart_dormancy` (BQ-07) | one row per dormant customer_id per month | Dormancy | Gold mart |
| `mart_daily_flows` (BQ-08) | one row per date | Liquidity | Gold mart |
| `mart_pipeline_health` (BQ-10) | one row per (pipeline run, source) | Run metadata / reconciliation | Gold mart (mandatory) |

Exact `pipeline/silver/*.py` and `pipeline/gold/*.py` filenames are assigned in Fasa C/D as each
table is built; this table is the pre-build contract each file must match (checked by
`gates/doc_reference_contract.py` once those files exist).

## Clean-model doctrine (non-negotiable regardless of stack)
- 1 table = 1 grain = 1 business entity — no mixed-domain dimensions.
- Bridge tables (not CTEs) for N:N relationships — e.g. a customer-to-account bridge where Berka's
  `disp` table already encodes disponent/owner N:N; reuse `disp`'s shape rather than re-deriving it. In the Salesforce delivery (ADR-006 Add #2) this
  same N:N rides the native `AccountContactRelation` object — a real bridge table, not a CTE,
  keeping this doctrine intact end-to-end.
- Serving layer = view, never a duplicated physical copy of Gold (Snowflake external tables in
  Fasa E read Gold S3 directly — no copy-in).
- One isolated SCD strategy per table: **Type 1 (overwrite) for `dim_customer`** — survivorship
  re-resolves the golden record each run; **no SCD (snapshot/append-only) for facts** — a
  transaction/application row never changes after landing; **Type 2 explicitly NOT used
  anywhere in v1** (named as deliberately out — see ADR-005 consequences).
- What's deliberately OUT: any mart beyond the 10 BQs (see journey/02 "Explicitly out of
  scope"), real-time freshness SLAs, hard-delete-complete facts until the Fasa C CDC upgrade
  (ADR-004). Also OUT (named, not silently absent, per this doctrine): **address-change history /
  velocity** — Salesforce (ADR-006 Add #2) makes Contact/Account address changes observable, but the
  correct model for it is a NEW append-only fact_address_change event fact (grain: one row per
  observed address change per customer_id), NOT a Type 2 SCD on `dim_customer` (which stays Type 1);
  that capability + fact are pending `@scope-guardian` ADR-000 intake, so no such table exists in v1.

## ERD / diagram
No `.dbml`/graphical ERD in v1 — the grain-declarations table above plus the join paths named in
`journey/02_BUSINESS_QUESTIONS.md` (per-BQ "sources joined" column) constitute the model of record.
A diagram may be added later as a journey/08 evidence artifact; not required for gates.

## Identity / grain fidelity
Cross-source identity = `dim_customer_xwalk` (ADR-005, D-04): one bank-wide `customer_id` per real
person, mapped to each SEEDABLE source's native key. Within a single source, native keys are the
identity (`SK_ID_CURR`, `nameOrig`/`nameDest`, `client_id`+`account_id`) — no content-hash
identity is needed here (unlike CIL's near-duplicate-video problem); the hard problem is
cross-system resolution, not de-duplication of near-identical content. This is the `identity:`
block already filled in `gates/framework.yml`. (OBP `account_id` is a Silver-only native key on
`sil_obp_accounts`, NOT a crosswalk member — OBP is deliberately Silver-terminal, ADR-005 Add #2.)

**Teradata/Bank Marketing exception (ADR-006 D6.2, R-38)**: this source has no native key at all —
it is not a 5th independent identity to resolve, it is deterministically ASSIGNED an existing
`customer_id` at seed time (sampled without replacement from the xwalk population). `sil_campaign_
response` therefore joins to `dim_customer` via `customer_id` directly, with no separate native-key
column — this is stated explicitly so a future reader doesn't mistake it for a 6th real identity
system.
