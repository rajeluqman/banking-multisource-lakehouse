# 01 — Dataset & Sources

> Content source: `00_MASTER_SPEC.md` (source table) + `03_DATASET_RISKS_AND_RESOLUTIONS.md`
> (R-01…R-22, source-specific rows) in the planning lab
> (`/workspaces/creative_intelligence_lab/architecture/banking_lakehouse_lab/`).

## Source inventory
| Source | Type | Owner/access | Licence/PII constraint | Refresh cadence |
|---|---|---|---|---|
| PostgreSQL (Docker, "Sales / Loan Dept") | DB export, seeded from Kaggle "Home Credit Default Risk" | Simulated internal system; seeded once, then `drip_feed.py` INSERT/UPDATE | Kaggle competition licence (research/portfolio use); anonymized `SK_ID_CURR`, no direct PII, but income/employment fields are financially sensitive | Incremental, high-watermark, on-demand run |
| MS SQL Server 2022 (Docker, "Credit Card + Fraud") | DB export, seeded from Kaggle "PaySim" synthetic mobile-money simulator | Simulated internal system; seeded once, then drip-fed | Synthetic data, no real PII; `nameOrig`/`nameDest` are synthetic codes | Incremental, high-watermark, on-demand run |
| SAP-sim file drop (`sap_drop/`, "Internal CRM") | File dump (CSV), seeded from Kaggle/PKDD'99 "Berka — Czech Financial Dataset" | Simulated legacy SFTP-style export; seeded once, then re-dropped/updated by drip-feed | Real-shaped Czech national-ID (`birth_number`) encoding DOB+gender — treated as sensitive (D-16, R-27) even though the underlying dataset is a public 1999 research release | File-drop pickup, manifest-tracked |
| Open Bank Project sandbox ("Core Banking API") | REST API (OAuth/DirectLogin), nested JSON | Free public sandbox (no seed — generates its own accounts/balances/transactions) | Sandbox data, no real customer PII; may reset without notice (R-21) | On-demand pull, paginated |
| BNM OpenAPI (optional enrich) | REST API, no auth | Genuinely live public FX/rate data | Public, no PII | Daily, optional — never a build dependency (D-12) |

**Rejected sources** (named, with reasons — `01_OPUS_DECISIONS.md` REJECTED list):
`ga4_obfuscated_sample_ecommerce` (static Nov 2020–Jan 2021, Gemini's "live" claim was factually
wrong), Wise sandbox (auth ceremony buys nothing over OBP), SAP BTP trial / ABAP Docker (90-day
wall, 16–32GB RAM — a file-export simulation is both cheaper and more realistic for legacy SAP
integration than standing up a real SAP instance).

## Access mechanics
- **Postgres / MS SQL**: `docker-compose.yml` at repo root brings up both containers; `seed/`
  loaders (seed/postgres/load_home_credit.py, seed/mssql/load_paysim.py) populate them once
  from the Kaggle CSVs. Fasa B extractors (pipeline/extract/postgres_extract.py,
  pipeline/extract/mssql_extract.py) pull incrementally via `WHERE updated_at > :watermark`.
- **SAP-sim**: seed/sap_sim/load_berka.py writes the initial CSV set into `sap_drop/`;
  `drip_feed.py` periodically re-drops updated extracts (simulating a legacy nightly export).
  pipeline/extract/sap_drop_pickup.py picks up new/changed files by manifest (filename + sha256
  checksum), same pattern CIL used for Bronze idempotency.
- **OBP**: pipeline/extract/obp_client.py — OAuth2/DirectLogin, credentials from a Databricks
  secret scope in the canonical run, `.env` (gitignored) in the local dev loop. Never logged, never
  landed in the landing path itself (R-18/R-19, journey/09 §1).
- Auth for all of the above: see `journey/09_SECURITY_AND_ACCESS.md` §1 (Secrets management) —
  DB creds and the OBP token never live in code or committed config.
- Rate limits / cost: OBP sandbox has soft rate limits (R-20 — retry+backoff); no metered cost for
  Postgres/MSSQL/SAP-sim (self-hosted Docker). Databricks/Snowflake compute cost is `@finops-agent`'s
  watch, not a per-pull cost here.

## Volume & shape
- Home Credit: ~307K applications, 7 relational tables (application, bureau, bureau_balance,
  previous_application, POS_CASH_balance, credit_card_balance, installments_payments); the largest
  child table (`installments_payments`) is ~13.6M rows in the full Kaggle release (R-05) — seeded
  as a deterministic subset for the dev loop, full set for the canonical run (D-14).
- PaySim: ~6.3M synthetic transactions, single flat table + `isFraud`/`isFlaggedFraud` labels (R-08).
- Berka: 8 relational tables (client, account, disp, card, loan, trans, district, + the crosswalk
  we generate), spans 1993–1998 in the original release (R-13, date-rebased at seed per D-03).
- OBP: small, sandbox-scale, resets are possible (R-21) — treated as snapshot-append only, not the
  volume story (paysim carries volume).
- Known source-side quality issues (full detail + resolution in `06_DQ_PLAN.md`, tagged by R-id):
  no timestamps in Home Credit (R-01), orphan FKs (R-03), null-heavy anonymized columns (R-04),
  PaySim's simulation-hour `step` instead of real dates (R-06), `isFraud` vs `isFlaggedFraud`
  semantic confusion (R-08), merchant rows with by-design-empty balances (R-09), Berka's
  `birth_number` DOB+gender encoding (R-12), file-drop duplication/partial-write hazards (R-15,
  R-16), Czech diacritics/encoding (R-17), OBP token expiry/pagination truncation (R-18, R-22).

## Decision log for this doc
- **Chosen sources and why**: each of the 4 was picked to embody one distinct source-system
  archetype a real bank has — RDBMS OLTP export (Postgres/Home Credit), a second RDBMS with a
  different vendor shape (MSSQL/PaySim, chosen specifically for its fraud label), a legacy
  file-drop integration (SAP-sim/Berka, chosen because it's the ONLY one of the four with a real
  customer master + real-shaped national ID), and a live REST API (OBP, chosen over Wise for zero
  auth ceremony). No single dataset has all four shapes, and no common key exists across any of
  them — that gap IS the project's keystone problem (R-23, D-04).
- **Why not one bigger single-source dataset instead**: a single source produces a normal ETL
  portfolio piece; the resume claim this project defends is specifically about multi-source MDM
  and heterogeneous-source ingestion — a single source can't produce that story.
