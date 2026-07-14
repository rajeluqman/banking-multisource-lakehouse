# Next Build Kickoff — Salesforce CRM swap (source #4) + Fasa A→D live

> Paste-able kickoff for a fresh **Sonnet** session in the owner's dedicated Codespace.
> The ARCHITECTURE + DESIGN + SCOPE for this round are DECIDED and gated green — this doc is
> EXECUTION (code) only. Do NOT re-litigate `ADR-006` Addendum #2 or the ingest-not-federate
> ruling; if something in the docs looks wrong once you're reading real code, STOP and surface it,
> don't silently improvise (this repo's CLAUDE.md anti-shortcut rule).

## What changed and why (one paragraph)
Source #4 moved from SAP HANA Cloud → **Salesforce** (Developer Edition). It's still the "CRM"
role; **Berka is still the seeded data + golden-record keystone** (ADR-005 L26) — only the host
and extractor change. Ingestion is **Salesforce Bulk API 2.0 + `SystemModstamp` watermark
incremental INTO the medallion** (Landing→Bronze→Silver→Gold) — NOT federated direct-query (a
verified anti-pattern), NOT Pub/Sub CDC, NOT Airbyte. Teradata (source #5) is UNCHANGED (keeps its
hand-built trigger `_cdc_log` CDC + cold-tier). BQ-03's old synthetic CRM-ticket proxy is replaced
by a real Salesforce **Case** timestamp.

## Read first, in this order
1. `governance/ADR/ADR-006-real-sap-hana-teradata-cdc-showcase.md` **Addendum #2** — the swap, the
   source-key rename decision, and the scope rulings.
2. `journey/01_DATASET_AND_SOURCES.md` (source #4 rows), `journey/05_STTM.md` (Berka→Salesforce
   object mapping + new `sil_crm_case`), `journey/07_PIPELINE_SPEC.md` (Bulk API mechanism,
   prerequisites), `journey/09_SECURITY_AND_ACCESS.md` (`birth_number__c` masking).
3. `governance/BOUNDARY_CONTRACT.md` (sanctioned ingestion = Salesforce Bulk API 2.0 client).
4. `BUILD_REPORT.md` — what's already built and what's UNVERIFIED. Nothing here has run against a
   live Salesforce org yet — do not assume a working Bronze/Silver blind.

## Prerequisite (owner, before live run)
Salesforce Dev Edition provisioned + a Connected App (OAuth) + security token; `.env` filled with
`SALESFORCE_LOGIN_URL / CLIENT_ID / CLIENT_SECRET / USERNAME / PASSWORD / SECURITY_TOKEN` (see
`.env.example`). If not yet provisioned, build against the dev-loop fallback and mark the relevant
`BUILD_REPORT.md` rows UNVERIFIED — do not fake a live-run proof.

## Task list (dependency order — each cites the governing doc)

1. **Rename internal source key `sap_hana` → `salesforce` (ADR-006 Add #2 "Internal
   source-identifier key").** One mechanical pass across ALL ~12 files so Bronze path segments,
   watermark keys, and the health-mart source→silver map stay consistent:
   `pipeline/silver/silver_crm.py`, `pipeline/promote/promotion_gate.py`,
   `pipeline/gold/mart_pipeline_health.py`, `pipeline/gold/dim_customer.py`,
   `pipeline/orchestrate_config.yml`, `drip_feed.py`, and the comment-only refs in
   `pipeline/extract/{cdc_common,cdc_initial_snapshot,teradata_extract}.py`, `seed/common/cdc_ddl.py`.
   `grep -rn "sap_hana" .` must return zero live-code hits when done (docs/history excepted).

2. **Seed loader — `seed/salesforce/load_berka.py` (replaces `seed/sap_hana/load_berka.py`).**
   Load Berka CSVs into Salesforce standard objects via Bulk API 2.0: `client`→**Contact**
   (with `birth_number__c`, `berka_client_id__c` external-id custom fields), `account`→**Account**,
   `disp`→**AccountContactRelation** (native N:N — keep bridge-not-CTE), CRM tickets→**Case**.
   Deterministic MDM linkage (Berka has a real master): `customer_id` → `dim_customer_xwalk.
   berka_client_id` ↔ Contact `berka_client_id__c`. DELETE the old `seed/sap_hana/` after.

3. **Extractor — `pipeline/extract/salesforce_extract.py` (replaces `sap_hana_extract.py`).**
   Bulk API 2.0 job lifecycle (create query job → poll status → download CSV results) with a
   `SystemModstamp` high-watermark for incremental pulls, landing into Landing with the same
   manifest/`_SUCCESS` shape as every other source. Idempotent re-run (journey/07). DELETE the old
   `sap_hana_extract.py`. `cdc_common.py`/`cdc_initial_snapshot.py` now serve **Teradata only** —
   update their docstrings.

4. **Silver — update `pipeline/silver/silver_crm.py` + add `sil_crm_case`.** Read the Salesforce
   Bronze (source key `salesforce`); keep `birth_number` decode (now from Contact
   `birth_number__c`), D-07 masking unchanged. Add the new `sil_crm_case` transform per the STTM
   in `journey/05_STTM.md` — this feeds BQ-03 (`mart_fraud_followup`) with a real Case timestamp.

5. **Orchestration — `pipeline/orchestrate_config.yml`.** Replace the `sap_hana_extract` stage with
   `salesforce_extract`; set source #4 cadence per `journey/07` (`bulk_api_poll`, not `cdc_poll`).
   Update `drip_feed.py`: Salesforce change-simulation is record edits via the API, NOT a SQL
   trigger — adjust or note why source #4 drips differently from Teradata.

6. **Health mart — `pipeline/gold/mart_pipeline_health.py`.** Ensure the `salesforce`→`silver_crm`
   source map and watermark-key logic are correct after the rename (BQ-10 reconciliation must still
   hold).

## Gate before calling ANY of this done
```
python3 gates/journey_completeness.py
python3 gates/boundary_contract.py
python3 gates/doc_reference_contract.py
python3 gates/secrets_scan.py
python3 -m unittest discover tests
```
All four gates green + tests passing is the bar — not "I wrote the files." Run against a live
Salesforce org if provisioned and capture real output into `journey/08_SERVING_AND_EVIDENCE.md`;
if not, say so explicitly and mark `BUILD_REPORT.md` rows UNVERIFIED.

## Update before ending the session
`PROJECT_STATUS.md` "▶ RESUME HERE" + `BUILD_REPORT.md` — same discipline as every prior fasa.
