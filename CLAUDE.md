# banking-multisource-lakehouse — AI Context

> Auto-loaded by Claude Code every session. Governance kit adopted from
> `framework_template/` (creative_intelligence_lab). Planning record for this project lives in
> CIL at `architecture/banking_lakehouse_lab/` (00_MASTER_SPEC.md, 01_OPUS_DECISIONS.md,
> 03_DATASET_RISKS_AND_RESOLUTIONS.md, 04_BUSINESS_QUESTIONS.md, 06_SECURITY_MODEL.md) — that
> is the design record; this repo is the build.

## STOP-GATE — read before ANY data/model/lineage work
Before editing a model, seed, schema, storage path, or ingest script — or proceeding past a
lineage/identity question — you MUST:
1. **Open the governing doc first.** Grain/model → `journey/04_DATA_MODEL.md` +
   `governance/ADR/ADR-005-star-schema-gold-and-mdm-xwalk.md`. Stack boundary →
   `governance/BOUNDARY_CONTRACT.md` + `governance/ADR/ADR-002-ratified-stack.md`. Layer
   separation (Landing vs Bronze) → `governance/ADR/ADR-003-four-layer-medallion.md`. Batch vs
   CDC → `governance/ADR/ADR-004-batch-first-cdc-later.md`. Scope →
   `journey/02_BUSINESS_QUESTIONS.md` + `governance/BACKLOG.md`. Security/PII/RBAC →
   `journey/09_SECURITY_AND_ACCESS.md`. New/ad-hoc feature →
   `governance/ADR/ADR-000-feature-intake-protocol.md`.
2. **Validate before building downstream.** Run `python gates/journey_completeness.py`,
   `python gates/boundary_contract.py`, `python gates/doc_reference_contract.py`,
   `python gates/secrets_scan.py` — these are the binding checks, not judgement.
3. **If a rule and the request conflict, STOP and surface it** — cite the doc, ask
   `@staff-data-engineer` / `@scope-guardian` before writing code. Do NOT re-litigate a locked decision
   (D-01…D-16 in the CIL planning lab) — if a conflict is real, it needs an ADR addendum, not a
   silent workaround.

Enforced three ways: this prompt (soft), `.claude/hooks/governance_guard.py` (blocks edits to
governed files per `gates/framework.yml` → `governed_paths`), CI (`.github/workflows/ci.yml`
blocks the PR). Governance is code, not vigilance.

## ANTI-SHORTCUT PROTOCOL — read-before-touch, reconcile-before-done
1. **Read-before-touch** — never edit or assert about a file from memory; read it THIS turn.
2. **Enumerate, don't sample** — for "all N" tasks (all R-ids, all BQs, all journey docs), get N
   from ground truth before acting, re-count after.
3. **Reconcile-before-done** — before saying done/fixed, restate the request as a checklist with
   evidence (`file:line` / command output) per item. No evidence = "unverified," not "done."
4. **Tag assumptions** — any load-bearing claim not checked this turn is marked "(unverified)".
5. **The planning docs are a MAP; the files on disk are the TERRITORY.** If a dataset column/file
   named in the CIL planning lab isn't actually there, STOP and surface it — do not improvise
   silently (this already happened once: see "Known blocker" below).
6. **TWO-STRIKE rule (ADR-009, mechanical)** — if the same pipeline stage fails TWICE, or a fix
   "succeeded" yet the symptom persists, STOP all paid execution and invoke
   `@staff-data-engineer` as Incident Commander BEFORE any further cluster run. Born from a real
   6-attempt fix-fail loop (BUILD_REPORT.md §24): classify code/state/environment, verify the
   last fix at the ARTIFACT level (never trust run SUCCESS), enumerate the bug-class blast
   radius, reproduce for free (local Spark needs
   `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` here), THEN exactly one paid run.

## Project overview
**Domain**: Banking / multi-source data platform simulation.
**Problem**: Simulate a bank's data estate — 4 heterogeneous source systems that share no common
key (2 RDBMS, 1 file-drop legacy export, 1 REST API) — and build the real data-engineering layer:
incremental extraction → Landing → Bronze → Silver → Gold, with an MDM customer crosswalk
resolving identity across all four, DQ gates, and Customer-360/fraud/loan-funnel marts.
**Purpose**: Data-engineering portfolio project (single-dev), interview-defensible.

## Scope (see `journey/02_BUSINESS_QUESTIONS.md` + `governance/BACKLOG.md`)
v1 = exactly the 10 business questions (BQ-01…BQ-10). No AI search, no RAG, no dashboard product,
no ML model training, no streaming/CDC (Fasa C — a later, separate build), no Terraform, no
Fabric. A new mart or feature idea goes through `governance/ADR/ADR-000-feature-intake-protocol.md`
before any code — `@scope-guardian` holds hard veto.

## Stack (locked — see `governance/BOUNDARY_CONTRACT.md` + ADR-002)
| Layer | Storage | Compute/engine | Notes |
|-------|---------|-----------------|-------|
| Landing | S3 `banking/landing/` (transient, short TTL) / local-disk fallback | Databricks portable PySpark | ADR-003 |
| Bronze | S3 `banking/bronze/` (permanent, append-only Delta) | Databricks portable PySpark + Delta | ADR-003, D-05 |
| Silver | S3 `banking/silver/` (Delta, MERGE) | Databricks portable PySpark + Delta | D-07 masking here |
| Gold/marts | S3 `banking/gold/` (Delta), Unity Catalog governed | Databricks portable PySpark + Delta | ADR-005 star schema |
| Serving | Snowflake external tables over Gold S3 (or DuckDB $0 fallback) + Power BI | Snowflake / DuckDB | Fasa E, optional |

**Sources (5, ADR-006 owner override 2026-07-06):** Postgres (Docker, Home Credit) + MS SQL
Server (Docker, PaySim) batch watermark; **SAP HANA Cloud** (BTP Free Tier, Berka — replaces the
original file-drop simulation) + **Teradata** (UCI Bank Marketing, new) via a portable CDC-poll
pattern (trigger + `_cdc_log` change-table, NOT SAP SLT/SDI or QueryGrid — see
`governance/ADR/ADR-006-real-sap-hana-teradata-cdc-showcase.md`); Open Bank Project sandbox (REST
API). All source-side compute (Docker containers, SAP HANA Cloud/Teradata connections) runs in
whichever environment actually executes the pipeline — **not the docs/planning session**; this
main session writes code only, does not download datasets or run containers/connections itself.

Dev loop = local Spark / deterministic sample set (free, fast). Canonical full run = the
disposable Databricks trial (screenshot evidence, then delete) — D-01 Addendum #3. No DLT / heavy
`dbutils` / notebook-only magic on the critical path (`gates/boundary_contract.py` bans
`import dlt` repo-wide) — the transforms must survive the trial workspace being deleted.

## Architecture of record
`journey/` — the 9 mandatory docs (dataset/sources, business questions, DRD, data model, STTM, DQ
plan, pipeline spec, serving/evidence, security/access). `governance/ADR/` — ADR-000 (feature
intake), ADR-001 (security mandatory), ADR-002 (stack), ADR-003 (4-layer medallion), ADR-004
(batch-first/CDC-later), ADR-005 (star schema + MDM xwalk). `governance/BOUNDARY_CONTRACT.md`,
`governance/BACKLOG.md`.

**Governance gate:** `@staff-data-engineer` (Opus, top technical authority — merged Staff DE +
architect) holds ultimate veto on model/schema changes and enforces the Clean-ERD Doctrine
(1 table = 1 grain = 1 entity, bridge tables not CTEs for N:N, serving = view never a duplicated
table, one explicit SCD strategy per table), AND owns technical strategy / stack / buy-vs-build /
trade-off analysis for new features. `@scope-guardian` holds hard veto on scope creep. No
Gold/marts work proceeds without `@staff-data-engineer` sign-off.

## Cabinet (6 agents) — see `.claude/agents/`
`@staff-data-engineer` (Opus, top technical authority — model/schema ultimate veto + Clean-ERD
doctrine + stack/tool/trade-off strategy; merged the former `@architect` role) ·
`@scope-guardian` (hard veto on scope) ·
`@senior-data-engineer` (build, idempotency, perf) · `@data-quality-steward` (DQ plan, gates) ·
`@product-owner` (BQ definition-of-done) · `@finops-agent` (part-time — Databricks/Snowflake/
Kaggle-API cost watch) · `@cikgu` (optional teaching layer, run as MAIN session not a subagent —
copied verbatim from CIL's pattern).

## Known blocker (read before Fasa A) — PARTIALLY CLEARED 2026-07-16 (ADR-002 Add #5)
**Update (2026-07-16, live-tested):** the credential gaps below are now CLEARED. `.env` carries
working `KAGGLE_USERNAME`/`KAGGLE_KEY` (`kaggle datasets list` authenticates, exit 0) and working
AWS/Databricks creds — **real S3 writes are PROVEN end-to-end** (ADR-002 Addendum #5): Gold Delta
written to `s3://banking-lakehouse-pipeline/banking/gold/` on a `SINGLE_USER` cluster via
secret-scope S3A, verified by Databricks read-back + independent `boto3`. What is NOT yet done is
a full multi-source canonical INGEST (download all datasets → sources → Landing→…→Gold) — that is
a separate scoped effort (`@finops`/`@scope-guardian`), not a credential blocker.

*Original blocker text (kept for history — no longer true as of 2026-07-16):* No Kaggle API
credentials and no live AWS/Databricks/Snowflake credentials exist in this build environment. The
Kaggle CSVs (Home Credit, PaySim, Berka) are NOT on disk anywhere in this workspace — confirmed by
search, not assumed. See `PROJECT_STATUS.md` "▶ RESUME HERE" and `BUILD_REPORT.md` for exactly how
the dev-loop seeding was handled given this gap, and what the owner still needs to supply for the
canonical (real-data, real-cloud) run.

## Token discipline
1. Checkpoint first: read `PROJECT_STATUS.md` "▶ RESUME HERE" before reading code.
2. Scope reads to the current fasa/module — max ~3 files/turn where possible.
3. Use an Explore-style subagent for "where is X" instead of reading many files inline.
4. Update the checkpoint before ending a turn.

## What NOT to commit
`.env*`, real Kaggle CSVs / raw data dumps, credentials, Databricks/Snowflake tokens, generated
large binaries (`*.parquet` outside intentional seed fixtures), `data/` (local-disk S3 fallback).
