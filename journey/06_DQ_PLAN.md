# 06 — Data Quality Plan (DQD)

> Every row below is a `03_DATASET_RISKS_AND_RESOLUTIONS.md` risk tagged **DQ-gate** in the
> planning lab — this doc is the executable translation of that register into layer/check/tool/
> fail-action form. R-ids are cited so `BUILD_REPORT.md`'s final self-audit can trace each one.

## Gates per layer
| Layer | Check | Tool | Fail action |
|---|---|---|---|
| Landing→Bronze | `_SUCCESS` marker + manifest/checksum match (R-15, R-16) | pipeline/promote/promotion_gate.py (custom, transport-integrity only) | Block promotion — quarantine in Landing, Bronze untouched, alert |
| Landing→Bronze | Pagination reconciled vs API-reported totals (R-22) | pipeline/promote/promotion_gate.py | Block promotion — quarantine |
| Landing→Bronze | Dedup redelivered/re-dropped files (R-15) | pipeline/promote/promotion_gate.py (checksum-keyed manifest) | Skip duplicate, log, continue |
| Landing→Bronze | Multi-file set completeness (Berka's 8 tables, now Salesforce Account/Contact/Case/AccountContactRelation extracts) | pipeline/promote/promotion_gate.py | Block promotion until full set present |
| Landing→Bronze | Schema-drift detection (R-16, R-28) | pipeline/promote/promotion_gate.py (schema hash compare) | Block promotion + alert; controlled `mergeSchema` only after explicit review, never silent |
| Bronze→Silver | FK-integrity — orphan `bureau`/`previous_application`/`bureau_balance` rows (R-03), AND the 3 HC-1 child tables `installments_payments`/`credit_card_balance`/`pos_cash_balance` on `SK_ID_CURR`→`application` (ADR-005 Add #5) | custom PySpark DQ check (`build_hc_child_table` in `pipeline/silver/silver_sales.py` for the HC-1 tables) | Route to `_quarantine_<table>_orphans`; count + report, never silently drop. For the HC-1 tables the orphans (~14.8% of installments) are Home Credit TEST-set applicants (no `application` row, no `TARGET`) — quarantined here so they don't reach Gold as NULL-keyed rows |
| Silver→Gold | BQ-11 repayment-metric semantics (ADR-005 Add #5) — unpaid installments (NULL `days_entry_payment`/`amt_payment`) must count as late+underpaid (every installment is past-due; a never-paid one is the strongest delinquency signal, not "unknown"), and `cc_avg_utilization` must be clamped `>= 0` (a negative balance is an overpayment, not negative utilization) | asserted in `pipeline/gold/fact_repayment_behavior.py` + `journey/05_STTM.md` metric definitions | Any change that lets unpaid installments silently drop out of the rate denominators, or lets a negative utilization through, is a metric-correctness regression — the never-paid customer must not look clean |
| Bronze→Silver | Null-rate expectations per column (R-04) | custom PySpark DQ check (per-column threshold) | WARN below threshold; BLOCK above a hard ceiling (defined per column at implementation time) |
| Bronze→Silver | `birth_number` decode correctness (R-12) | unit test (tests/test_birth_number_decode.py) with known fixtures | Block Silver build on unit-test failure |
| Bronze→Silver | Merchant-row balance-null exception (R-09) | custom PySpark DQ check, scoped to entity_type=merchant | Excluded from the general null-rate check, not a failure |
| Bronze→Silver | Balance reconciliation (`oldbalance`/`newbalance` vs `amount`) (R-11) | custom PySpark DQ check | **WARN only** — documented known PaySim simulator quirk, data not silently "fixed" |
| Bronze→Silver | Encoding/diacritics check on Berka name/district fields (R-17) | custom PySpark DQ check (UTF-8 validity) | WARN + log; force UTF-8 re-encode at seed if source encoding is wrong |
| Bronze→Silver | `isFraud` vs `isFlaggedFraud` used correctly (R-08) | code review / STTM adherence check (not automatable at data level — asserted at Gold KPI definition) | Any Gold model reading `isFlaggedFraud` as the fraud KPI is a Clean-ERD Doctrine violation (architect veto) |
| Silver→Gold | Currency normalization completeness — every monetary column has a currency code before FX conversion (R-14) | `pipeline/gold/dq_currency_gate.py` (built this session — was previously documented but never implemented; wired into `pipeline/orchestrate_config.yml` ahead of `fact_txn`/`fact_card_fraud`) | Block Gold build if any monetary column lacks a currency tag — raises `CurrencyTagMissing`. `AMT_INCOME_TOTAL` (Home Credit) is checked for tag presence (`unitless`) but excluded from FX conversion (documented D-12 exception, @staff-data-engineer sign-off) |
| Silver→Gold | Late-arriving dimension unknown-member handling (R-29) | custom PySpark DQ check (count of `-1` customer_id rows, re-link job coverage) | WARN if unknown-member count grows run-over-run without re-linking |
| Any→Gold | Per-run source→Bronze→Silver→Gold row-count reconciliation (R-30) | pipeline/gold/mart_pipeline_health.py | Surfaced as the BQ-10 mart itself, not a separate hidden report |
| Landing→Bronze | Salesforce Bulk API watermark boundary re-pull dedup — `SystemModstamp >` re-pull deduped on `Id`+`SystemModstamp` (R-36 revised, ADR-006 Add #2) | pipeline/promote/promotion_gate.py |
| Landing→Bronze | Teradata CDC dedup — redelivered/out-of-order `_cdc_log` events (R-37, ADR-006) | pipeline/promote/promotion_gate.py (dedup key: `pk_value` + `op` + `seq`) | Same promotion gate as every other source — dedup, don't drop; Bronze sees exactly-once |
| Bronze→Silver | Bank Marketing linkage coverage — what fraction of `dim_customer_xwalk` actually got a campaign-response row (R-38) | custom PySpark DQ check (count + %, not a pass/fail threshold) | WARN + report the coverage %; a thin sample-set dev-loop population is expected to have LOW coverage, not a defect |
| Bronze→Silver | Home Credit `TARGET` vs Bank Marketing `default` disagreement rate (BQ-05, ADR-006) | custom PySpark DQ check | Report the disagreement rate; never silently pick one signal as "correct" over the other |
| Landing→Bronze | Seed-time bulk load never reaches `_cdc_log` (R-40, ADR-007 D7.5) — Teradata's CDC triggers only fire on changes AFTER install, so the initial seeded rows are invisible to `poll_cdc_log` (source #4/Salesforce is NOT affected — its `SystemModstamp > :watermark` first run with an epoch/null watermark naturally full-pulls the seeded rows; ADR-006 Add #2) | one-time "initial snapshot" extraction for the Teradata CDC source, same shape as `jdbc_batch_common.py`'s first-run full-pull | Block: Teradata's CDC extractor must not be treated as complete until the initial snapshot has been promoted at least once |

## LLM/ML-output-specific gates
N/A — 2026-07-05, reason: this pipeline is fully deterministic ETL/ELT (PySpark transforms over
seeded/extracted relational and REST data). No LLM extraction or ML scoring step exists anywhere
in the locked scope (journey/02 "Explicitly out of scope" #4 — no model training). `isFraud`/
`TARGET` are pass-through labels from the source data, not model output.

## PII / sensitive-field handling
Mask order: raw PII lands verbatim in Landing→Bronze (D-05/D-15, access-restricted — R-27) →
decoded/masked exactly once at the Bronze→Silver gate (D-07) → Gold and serving see only the
masked/decoded form, never re-exposed. Governing ADR: `governance/ADR/ADR-005-star-schema-gold-
and-mdm-xwalk.md` for the identity model this masking sits on top of; full classification +
handling table in `journey/09_SECURITY_AND_ACCESS.md` §2 (D-16).

## Known accepted quality gaps
| Gap | Accepted reason | Date |
|---|---|---|
| PaySim `oldbalance`/`newbalance` don't always reconcile with `amount` (R-11) | Known, documented PaySim simulator artifact — not a pipeline defect; fixing it would fabricate data the source never had | 2026-07-05 |
| Hard deletes invisible to Fasa B watermark batch (R-25, ADR-004) | Named limitation of batch-first ingestion; closed by the Fasa C CDC upgrade, not a v1 defect | 2026-07-05 |
| BQ-03 (fraud→CRM follow-up SLA) has no real ticketing source among the 4 systems (journey/03) | Documented gap — resolved via a stated proxy or marked partially-simulated at Fasa D build time, never silently invented — SUPERSEDED 2026-07-14: now resolvable via a real Salesforce `Case` timestamp (ADR-006 Add #2, BQ-03 enrichment); the synthetic proxy is no longer required | 2026-07-05 |
| No Delta `OPTIMIZE`/Z-ORDER compaction step anywhere in the pipeline (R-41) — `pipeline/extract/cdc_common.py`'s poll pattern (ADR-006 D6.3) writes one small Landing partition per poll, and `pipeline/promote/promotion_gate.py` appends each promoted poll to Bronze as its own small write; over many polls this is the classic "small files problem," which will slow Silver `MERGE` reads (`pipeline/silver/common.py`) and, once live, Snowflake/DirectQuery reads over Gold | Named this session (owner traced it from a general CDC-pipeline discussion, mapped correctly to this repo's Delta stack — not Iceberg). Not part of any ADR-006/007 task list; storage-format choice (Delta, ADR-002) already supports the fix natively (`OPTIMIZE`/`ZORDER BY`), so no new tool/ADR is needed to close it — just an unbuilt maintenance stage. Deferred: owner chose to record, not build, this session | 2026-07-10 |
