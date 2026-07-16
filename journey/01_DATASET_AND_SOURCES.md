# 01 — Dataset & Sources

> Content source: `00_MASTER_SPEC.md` (source table) + `03_DATASET_RISKS_AND_RESOLUTIONS.md`
> (R-01…R-22, source-specific rows) in the planning lab
> (`/workspaces/creative_intelligence_lab/architecture/banking_lakehouse_lab/`).

## Source inventory
| Source | Type | Owner/access | Licence/PII constraint | Refresh cadence |
|---|---|---|---|---|
| PostgreSQL (Docker, "Sales / Loan Dept") | DB export, seeded from Kaggle "Home Credit Default Risk" | Simulated internal system; seeded once, then `drip_feed.py` INSERT/UPDATE | Kaggle competition licence (research/portfolio use); anonymized `SK_ID_CURR`, no direct PII, but income/employment fields are financially sensitive | Incremental, high-watermark, on-demand run |
| MS SQL Server 2022 (Docker, "Credit Card + Fraud") | DB export, seeded from Kaggle "PaySim" synthetic mobile-money simulator | Simulated internal system; seeded once, then drip-fed | Synthetic data, no real PII; `nameOrig`/`nameDest` are synthetic codes | Incremental, high-watermark, on-demand run |
| **Salesforce** (Developer/trial org, "Internal CRM") | Real managed SaaS CRM, seeded from Kaggle/PKDD'99 "Berka — Czech Financial Dataset" into standard objects (Account/Contact/Case); Bulk API 2.0 incremental extraction (ADR-006 Add #2) | Owner-provisioned Salesforce org; Berka seeded once into Account/Contact, then change-tracked via `SystemModstamp` watermark | Real-shaped Czech national-ID (`birth_number`, now a Contact custom field `birth_number__c`) encoding DOB+gender — treated as sensitive (D-16, R-27) even though the underlying dataset is a public 1999 research release | Bulk API 2.0 pull, `SystemModstamp` high-watermark (ADR-006 Add #2) |
| **Teradata** (Vantage Express or Teradata Cloud free tier, "Marketing / Campaign") | Real managed/VM DB, seeded from UCI "Bank Marketing" dataset; CDC-style extraction (ADR-006) | Owner-provisioned; seeded once (deterministic sampled linkage to `dim_customer_xwalk`, R-38), then change-tracked | Public research dataset (Moro et al.), no direct PII — demographic fields (`job`/`education`/`marital`) classified confidential per journey/09_SECURITY_AND_ACCESS.md | CDC poll (trigger + `_cdc_log` change-table pattern, ADR-006 D6.3) |
| Open Bank Project sandbox ("Core Banking API") | REST API (OAuth/DirectLogin), nested JSON | Free public sandbox (no seed — generates its own accounts/balances/transactions) | Sandbox data, no real customer PII; may reset without notice (R-21) | On-demand pull, paginated |
| BNM OpenAPI (optional enrich) | REST API, no auth | Genuinely live public FX/rate data | Public, no PII | Daily, optional — never a build dependency (D-12) |

**Rejected sources** (named, with reasons — `01_OPUS_DECISIONS.md` REJECTED list):
`ga4_obfuscated_sample_ecommerce` (static Nov 2020–Jan 2021, Gemini's "live" claim was factually
wrong), Wise sandbox (auth ceremony buys nothing over OBP). **SAP BTP trial / ABAP Docker was
originally rejected here, then OVERRIDDEN by the owner 2026-07-06** — see
`governance/ADR/ADR-006-real-sap-hana-teradata-cdc-showcase.md` and the "Superseded rejections"
table in `governance/BACKLOG.md` (SAP HANA Cloud Free Tier only, not full ABAP/Netweaver;
trial-wall accepted as a non-issue for this owner's operating model).

## Access mechanics
- **Postgres / MS SQL**: `docker-compose.yml` at repo root brings up both containers; `seed/`
  loaders (seed/postgres/load_home_credit.py, seed/mssql/load_paysim.py) populate them once
  from the Kaggle CSVs. Fasa B extractors (pipeline/extract/postgres_extract.py,
  pipeline/extract/mssql_extract.py) pull incrementally via `WHERE updated_at > :watermark`.
- **Salesforce** (owner-provisioned, ADR-006 Add #2): seed/salesforce/load_berka.py authenticates via
  Client Credentials Flow (Consumer Key/Secret + My Domain host — NOT username-password, this org's
  External Client App model doesn't expose that flow, BUILD_REPORT.md §11) and loads Berka into
  `client`->Contact (`berka_client_id__c`/`birth_number__c`), `account`->Account, `disp`->
  AccountContactRelation, `trans`->**Transaction__c** (new custom object), `district`->
  **District__c** (new custom object), tickets->Case (seed-time synthetic — Berka has no native
  ticket table). `card`/`loan` are intentionally NOT loaded (build-scope note, seed/salesforce/
  load_berka.py docstring — neither is read by any Gold builder). There is NO `_cdc_log` trigger
  (OLTP SaaS, no DDL-trigger surface -- ADR-006 Add #2). pipeline/extract/salesforce_extract.py
  runs Bulk API 2.0 query jobs (`WHERE SystemModstamp > :watermark`), tracking its own `SystemModstamp` high-watermark in the lake.
- **Teradata** (owner-provisioned, ADR-006): seed/teradata/load_bank_marketing.py loads the UCI
  Bank Marketing rows (deterministic sampled linkage to `dim_customer_xwalk`, R-38), same
  `_cdc_log` + trigger pattern; pipeline/extract/teradata_extract.py polls via the same shared
  cdc_common.py logic (the trigger + `_cdc_log` CDC-poll pattern, D6.3; ADR-006 Add #2 moved only source #4 to Salesforce).
- **OBP**: pipeline/extract/obp_client.py — OAuth2/DirectLogin, credentials from a Databricks
  secret scope in the canonical run, `.env` (gitignored) in the local dev loop. Never logged, never
  landed in the landing path itself (R-18/R-19, journey/09 §1).
- Auth for all of the above: see `journey/09_SECURITY_AND_ACCESS.md` §1 (Secrets management) —
  DB creds and the OBP token never live in code or committed config. Salesforce
  OAuth (Connected App key/secret, refresh token) and Teradata connection details are supplied by the owner via `.env`, never pasted
  into chat/commits.
- Rate limits / cost: OBP sandbox has soft rate limits (R-20 — retry+backoff); no metered cost for
  Postgres/MSSQL (self-hosted Docker) or Salesforce/Teradata (owner's Developer-org / Free Tier accounts).
  Databricks/Snowflake compute cost is `@finops-agent`'s watch, not a per-pull cost here.

## Volume & shape
- Home Credit: ~307K applications, 7 relational tables (application, bureau, bureau_balance,
  previous_application, POS_CASH_balance, credit_card_balance, installments_payments); the largest
  child table (`installments_payments`) is ~13.6M rows in the full Kaggle release (R-05) — seeded
  as a deterministic subset for the dev loop, full set for the canonical run (D-14).
- PaySim: ~6.3M synthetic transactions, single flat table + `isFraud`/`isFlaggedFraud` labels (R-08).
- Berka: 8 relational tables (client, account, disp, card, loan, trans, district, + the crosswalk
  we generate), spans 1993–1998 in the original release (R-13, date-rebased at seed per D-03) —
  now seeded into a real Salesforce org (Account/Contact/Case standard objects) rather than a file-drop folder (ADR-006 Add #2).
- UCI Bank Marketing (Teradata): ~45,211 rows, single flat table (age, job, marital, education,
  default, balance, housing, loan, contact, campaign, pdays, previous, poutcome, `y`) — sampled
  down to `min(45211, xwalk customer count)` at seed via deterministic linkage (R-38).
- OBP: small, sandbox-scale, resets are possible (R-21) — treated as snapshot-append only, not the
  volume story (paysim carries volume).
- Known source-side quality issues (full detail + resolution in `06_DQ_PLAN.md`, tagged by R-id):
  no timestamps in Home Credit (R-01), orphan FKs (R-03), null-heavy anonymized columns (R-04),
  PaySim's simulation-hour `step` instead of real dates (R-06), `isFraud` vs `isFlaggedFraud`
  semantic confusion (R-08), merchant rows with by-design-empty balances (R-09), Berka's
  `birth_number` DOB+gender encoding (R-12), Czech diacritics/encoding (R-17), OBP token
  expiry/pagination truncation (R-18, R-22), Salesforce Bulk API watermark re-pull dedup (R-36, revised) and Teradata CDC duplicate/out-of-order events
  (R-37 — ADR-006; R-36 revised per ADR-006 Add #2), Bank Marketing's missing natural customer key (R-38), Free Tier network
  exposure misconfiguration (R-39 — Teradata only; Salesforce source #4 is a public SaaS endpoint with no network-exposure step, ADR-006 Add #2). Full R-36…R-39 detail: `governance/ADR/ADR-006-real-sap-hana-
  teradata-cdc-showcase.md` D6.5 — these are this repo's own additions, not in the original CIL
  planning-lab R-01…R-35 register.

## Decision log for this doc
- **Chosen sources and why**: originally 4 archetypes — RDBMS OLTP export (Postgres/Home Credit),
  a second RDBMS with a different vendor shape (MSSQL/PaySim, chosen for its fraud label), a
  legacy CRM integration (Berka, the only one with a real customer master + real-shaped national
  ID), and a live REST API (OBP, chosen over Wise for zero auth ceremony). No single dataset has
  all these shapes, and no common key exists across any of them — that gap IS the project's
  keystone problem (R-23, D-04).
- **2026-07-06 owner override (ADR-006)**: Berka's hosting mechanism moved from a simulated
  file-drop to a real SAP HANA Cloud instance, and a 5th source (Teradata, UCI Bank Marketing) was
  added — both specifically to teach real CDC-connector engineering (trigger + change-table
  pattern), not to reopen the underlying MDM/keystone argument above, which is unchanged and now
  proven across 5 disjoint systems instead of 4.
- **2026-07-14 owner override (ADR-006 Addendum #2)**: source #4's delivery vehicle moved from SAP
  HANA Cloud to a real Salesforce org — Berka is still the seeded data + golden-record keystone
  (ADR-005 L26 unchanged). Extraction changes from the trigger+`_cdc_log` CDC pattern (physically
  unavailable on OLTP SaaS) to Bulk API 2.0 + `SystemModstamp` watermark; the CDC-showcase skill is
  preserved by Teradata (source #5), which keeps trigger-CDC. Still 5 sources; the MDM/keystone
  argument above is unchanged.
- **Why not one bigger single-source dataset instead**: a single source produces a normal ETL
  portfolio piece; the resume claim this project defends is specifically about multi-source MDM
  and heterogeneous-source ingestion — a single source can't produce that story.
