# ADR-007 — Decoupled pipeline architecture, config-driven orchestration, historical-data strategy

**Status:** Accepted
**Date:** 2026-07-06
**Owners:** owner (ratified), architect (sign-off)
**Context refs:** extends `ADR-003-four-layer-medallion.md` (layer shape unchanged) and
`ADR-006-real-sap-hana-teradata-cdc-showcase.md` Addendum #1 (Teradata cold-tier). Does NOT
change the stack (`ADR-002`) — Databricks/S3/Snowflake stays; this ADR is about how the
*pipelines within that stack* are organized and sequenced, not what runs them.

## Context
Owner asked for the architecture to look like a real bank's decoupled pipeline estate —
many small, fault-isolated components per layer, not one monolithic script — and separately
raised three historical-data problems every production pipeline eventually hits (initial
load vs incremental, partition pruning, hot/cold storage economics). Both threads converge
on the same fix: this repo's current pipeline/silver/build_silver.py (since deleted — see
Consequences) was a single file doing all Silver tables, which is the opposite of decoupled,
and none of Gold's fact tables are partitioned, which is the opposite of query-efficient at
scale.

## Decision

### D7.1 — Silver splits into 5 domain-scoped pipelines (was 1 file)
pipeline/silver/build_silver.py is replaced by 5 independently-runnable modules, one per
source domain, each with its own DQ checks embedded (not a separate top-level DQ stage —
see D7.2 for why):
- pipeline/silver/silver_sales.py (Home Credit: application, bureau, previous_application)
- pipeline/silver/silver_fraud.py (PaySim: card_txn)
- pipeline/silver/silver_crm.py (Berka/SAP HANA: client + birth_number decode, account,
  disp, card, loan, trans, district)
- pipeline/silver/silver_marketing.py (Teradata: campaign_response)
- pipeline/silver/silver_core_banking.py (OBP: obp_accounts, obp_transactions)

Shared logic (`merge_upsert`, `mask_last4`, `orphan_quarantine`) stays in
`pipeline/silver/common.py`, imported by all 5 — the split is about fault isolation and
independent scheduling, not code duplication. If `silver_crm` fails (e.g. a birth_number
decode spike), `silver_fraud`/`silver_sales`/etc. are unaffected and still complete.

### D7.2 — DQ checks stay embedded per-domain-pipeline, NOT a separate top-level stage
Considered and rejected: a single shared "DQ Gate" stage between Silver and Gold. Rejected
because each domain's real DQ rules are domain-specific and already look different (R-03
orphan quarantine for bureau, R-08 fraud-label separation for card_txn, R-12 birth_number
decode for client) — forcing them through one shared gate would either genericize them into
uselessness or turn the "shared" gate into a dispatch table back to per-domain logic, which
is the same code with an extra hop. The medallion's OWN layer boundary (Bronze→Silver IS the
content-quality gate, ADR-003) already is the DQ checkpoint; a same-shaped fifth layer adds
ceremony without adding a new guarantee.

### D7.3 — Config-driven Master Orchestrator (D-10 compliant — no private Airflow)
New pipeline/orchestrate_config.yml (same philosophy as `gates/framework.yml` — one config,
scripts read from it, nobody hardcodes the DAG shape in Python) declares:
- The dependency graph: `[postgres_extract, mssql_extract, sap_hana_extract,
  teradata_extract, obp_client] → promotion_gate → [silver_sales, silver_fraud, silver_crm,
  silver_marketing, silver_core_banking] → [dim_customer_xwalk, dim_customer, dim_date,
  fact_txn, fact_card_fraud, fact_loan_application] → [9 marts]`.
- Cadence per source: `batch` (Postgres/MSSQL — scheduled, e.g. nightly) vs `cdc_poll`
  (SAP HANA/Teradata — short interval, e.g. every 5 min), so the orchestrator doesn't treat
  a continuous CDC poller the same as a once-nightly batch job.
- pipeline/orchestrate.py reads this config, runs each stage only after its upstream
  stage(s) succeeded, and writes a run-status row (stage, status, timestamp, error) into the
  SAME control-plane store `pipeline/common/watermark.py` already uses — `mart_pipeline_health`
  (BQ-10) reads this alongside its existing row-count reconciliation, so BQ-10 becomes a live
  reflection of orchestration health, not just a data-count check.
- This is NOT Airflow and does not compete with the control-plane contract this repo already
  exposes for `airflow_dag_running_pipeline` to adopt later (`journey/07_PIPELINE_SPEC.md`
  "Orchestration") — it's the local dev-loop sequencer, same spirit as the existing Makefile,
  made dependency-aware instead of a flat list of targets.

### D7.4 — Historical-data strategies, mapped onto THIS stack (not Fabric/OneLake)
| Strategy | Where implemented |
|---|---|
| **1. Initial load + incremental backfill** | Already present (`pipeline/extract/jdbc_batch_common.py`'s watermark-or-full-pull branch) — made EXPLICIT via a `--full-backfill` CLI flag on `postgres_extract.py`/`mssql_extract.py`, so a deliberate historical re-pull doesn't require manually deleting watermark state. |
| **2. Partition pruning** | Gap fix: `pipeline/gold/fact_txn.py` and `fact_card_fraud.py` add `.partitionBy("txn_year", "txn_month")` on write (currently unpartitioned — a real gap, not a style choice). Landing already partitions by `dt=` (ADR-003); this extends the same principle to Gold. |
| **3. Hot/cold hybrid** | `ADR-006` Addendum #1 — Teradata's dual-role cold-tier (native aggregated view + Power BI composite model), NOT a generic S3 lifecycle policy (rejected as less concrete — Teradata is already in this architecture and is historically an EDW technology, so using it AS the cold tier is more honest and more demoable than inventing a lifecycle-policy stand-in). |

### D7.5 — Named gap surfaced by this discussion: CDC path never captures the seed-time bulk load (R-40)
Found while reasoning through Strategy 3's cutover: seed/sap_hana/load_berka.py (since
DELETED, ADR-006 Add #2 — Berka's Salesforce successor is seed/salesforce/load_berka.py) and
`seed/teradata/load_bank_marketing.py` both `INSERT` the bulk seed data BEFORE calling
`setup_cdc()` — so the initial rows never fire the `AFTER INSERT` trigger and are **never
represented in `_cdc_log`**. `pipeline/extract/cdc_common.py`'s `poll_cdc_log` only reads
`_cdc_log`, so today the seed-time bulk load would never reach Landing/Bronze through the
CDC extractors at all. **Fix**: each CDC-source seed loader must ALSO perform a one-time
"initial snapshot" extraction (a plain full read of the base table, same shape as
`jdbc_batch_common.py`'s first-run full-pull) into Landing, promoted through the normal
gate, immediately after seeding — before the CDC pollers start. Tracked as **R-40** in the
risk register (this repo's own additions, alongside R-36…R-39).

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| Keep Silver as one monolithic file | Opposite of the decoupled, fault-isolated architecture the owner asked for; one domain's failure blocks all others |
| Separate top-level DQ Gate stage | Genericizes domain-specific DQ rules or just adds a dispatch hop back to the same per-domain logic (D7.2) |
| Full Airflow/scheduler inside this repo | D-10 explicitly forbids a private Airflow here — this repo exposes a control-plane contract for the separate `airflow_dag_running_pipeline` project instead |
| Generic S3 lifecycle policy for Strategy 3 | Works, but less concrete/demoable than using Teradata (already in the architecture) as a real EDW-shaped cold tier |

## Consequences
- pipeline/silver/build_silver.py is DELETED (done this build session), replaced by 5 files
  — any doc referencing the old filename needs updating (checked by
  `gates/doc_reference_contract.py`).
- `pipeline/gold/fact_txn.py`/`fact_card_fraud.py` gain a partition column derivation step
  (`txn_year`, `txn_month`) not previously present.
- New files: pipeline/orchestrate_config.yml, pipeline/orchestrate.py,
  pipeline/gold/cold_tier/teradata_cold_view.sql.
- `mart_pipeline_health.py` (BQ-10) gains a new data source (orchestrator run-status) —
  additive, does not change its existing row-count reconciliation logic.
- Does NOT decide the exact CDC cutover date per table, or the exact aggregation grain of
  the Teradata cold view beyond "aggregate, not row-level" — implementation detail for the
  build step.

## Addendum log
- **2026-07-06 (Addendum #1) — All 7 `NEXT_BUILD_KICKOFF.md` tasks implemented (code only,
  same "code first, run later" split as every prior fasa — no live DB/cloud connections in
  this session).** D7.1 done (pipeline/silver/silver_sales.py, silver_fraud.py, silver_crm.py,
  silver_marketing.py, silver_core_banking.py; build_silver.py deleted). D7.3 done
  (pipeline/orchestrate_config.yml + pipeline/orchestrate.py). D7.4 Strategy 1 done
  (`--full-backfill` flag). D7.4 Strategy 2 done (`.partitionBy("txn_year", "txn_month")` on
  fact_txn.py/fact_card_fraud.py). D7.4 Strategy 3 / ADR-006 Addendum #1 done
  (pipeline/gold/cold_tier/teradata_cold_view.sql). D7.5/R-40 done
  (pipeline/extract/cdc_initial_snapshot.py, wired into both CDC-source seed loaders) — see
  BUILD_REPORT.md for the one follow-on gap this surfaced (the initial-snapshot Bronze table
  isn't yet UNIONed into silver_crm.py/silver_marketing.py's CDC-log read, since that wiring
  wasn't part of this ADR's task list).
- **2026-07-06 (Addendum #2) — verifying-architect review found D7.3's cadence field was
  decorative.** `orchestrate.py` read `cadence` off each stage into a dict and never looked
  at it again — every stage (batch, cdc_poll, on_upstream alike) ran exactly once per
  invocation, which is precisely "treats a continuous CDC poller the same as a once-nightly
  batch job," the thing D7.3 said the orchestrator must NOT do. Fixed: `orchestrate.py`
  gained `--poll-seconds N` — after the first full pass, only `cdc_poll`/`on_upstream` stages
  re-run on each tick; `batch`-cadence extraction stages are deliberately NOT re-run by the
  loop (a real nightly pull is triggered by the next external invocation, e.g. cron, not by
  this process looping) — this still does not grow a private always-on Airflow-shaped
  scheduler for every stage (D-10), only `cdc_poll` stages and their dependents get interval
  behavior. Verified with a mocked-module test (no live Spark/DB needed): a fake `batch`
  stage ran once across 1 full pass + 2 poll ticks, a fake `cdc_poll` stage ran 3 times, and
  a fake `on_upstream` stage depending on both ran 3 times without being falsely blocked by
  the batch stage's absence on tick passes.
