# Learning Curriculum — banking-multisource-lakehouse

> Owned by @cikgu. The learning PATH for this project: each module pairs a concept with the
> real artifact that embodies it, the WHY-before-HOW questions you must answer first, and a DIY
> build task. Teach in order; close each module with a `LEARNING_LOG.md` entry.
> Run @cikgu as a MAIN session (not a subagent) for actual teaching.

**Score:** start 100. Track in `LEARNING_LOG.md`. Hint = -5. < 60 forces a docs break.

> One row = one teachable module, grounded in a REAL artifact on disk. cikgu reads ONLY the
> current module's artifact(s) per session. If a listed artifact is missing on disk, cikgu STOPS
> and surfaces it (map vs territory) — it does NOT improvise a lesson from memory.

| # | Module | You must be able to answer (WHY) | Artifact (read AFTER you've tried) | DIY |
|---|--------|----------------------------------|-----------------------------------|-----|
| **M0** | The domain & the goal | 4–5 source systems share NO common key (2 RDBMS, SAP HANA, Teradata, 1 REST API). Why is that the whole problem? What are the 4 medallion layers? | `journey/01_DATASET_AND_SOURCES.md`, `governance/ADR/ADR-003-four-layer-medallion.md` | — |
| **M1** | Landing vs Bronze | Why split a transient Landing from a permanent append-only Bronze? What breaks if you merge them? Why is Bronze the "re-derive, never re-extract" firewall? | `governance/ADR/ADR-003-four-layer-medallion.md` | explain the extraction firewall in 3 sentences |
| **M2** | Batch-first + CDC-poll | Why watermark-batch first, CDC later (ADR-004)? How do you fake CDC on SAP HANA/Teradata with a trigger + `_cdc_log` table WITHOUT SLT/SDI/QueryGrid? | `governance/ADR/ADR-004-batch-first-cdc-later.md`, `governance/ADR/ADR-006-real-sap-hana-teradata-cdc-showcase.md` | sketch the watermark-extract loop (skip-existing) |
| **M3** | Silver: MERGE + PII masking | Why mask at Silver, not Bronze or Gold (D-07/D-16)? Why MERGE not overwrite? What is idempotency here? | `journey/09_SECURITY_AND_ACCESS.md`, `journey/06_DQ_PLAN.md` | build a Silver MERGE-upsert skeleton DIY |
| **M4** | MDM customer crosswalk | No shared key — so how does one human become ONE `customer_id` across 4 sources? Why is `build_xwalk.py` the keystone (D-04)? | `governance/ADR/ADR-005-star-schema-gold-and-mdm-xwalk.md`, `seed/build_xwalk.py`, `journey/04_DATA_MODEL.md` | build `dim_customer_xwalk` resolution logic DIY, diff vs ref |
| **M5** | Gold star schema (Clean-ERD) | Why star not snowflake? "1 table = 1 grain = 1 entity", bridge tables not CTEs for N:N — why die on that hill? | `governance/ADR/ADR-005-star-schema-gold-and-mdm-xwalk.md`, `journey/04_DATA_MODEL.md` | draw the ERD from memory; check vs the data model |
| **M6** | DQ gates per layer | What do you test at each layer, and why does the test differ by layer? Why quarantine, not fail-the-run? | `journey/06_DQ_PLAN.md` | write one per-layer expectation DIY |
| **M7** | Serving layer | Why Snowflake external tables OVER Gold S3 (or DuckDB $0 fallback) — a view, never a copied table? What does ADR-010 say about the serving-pattern boundary? | `journey/08_SERVING_AND_EVIDENCE.md`, `governance/ADR/ADR-010-lakehouse-maturity-compaction-and-serving-patterns.md` | explain "serving = view never a duplicated table" |
| **M8** | The 10 business questions | What is "done"? Why is scope frozen at exactly BQ-01…BQ-10 (Customer-360 / fraud / loan funnel)? | `journey/02_BUSINESS_QUESTIONS.md`, `governance/BACKLOG.md` | map one BQ to the marts that answer it |
| **M9** | Governance as code | Why encode governance in hooks/gates/ADR-000 instead of trusting discipline? What is the two-strike incident rule and why does it exist? | `governance/ADR/ADR-000-feature-intake-protocol.md`, `governance/ADR/ADR-009-two-strike-incident-protocol.md`, `gates/` | add/read one gate and explain what it catches |
| **M10** | External orchestration: control-plane vs data-plane | Why does Airflow trigger-and-poll instead of computing the medallion itself (D-10)? Why is the DAB job fired as ONE `DatabricksRunNowOperator`, not 22 individual task-keys — what breaks if you re-implement that graph in Airflow? Why does source→Landing extraction run on the Airflow worker but nothing past Landing does? | `governance/ADR/ADR-011-external-airflow-orchestration-and-terraform-scope.md` (this repo — read the Addendum #2 correction, it supersedes the body's topology diagram), `../banking-multisource-lakehouse-airflow-dag/README.md`, `../banking-multisource-lakehouse-airflow-dag/journey/01_TOPOLOGY_DECISION.md` (the why-two-repos / D-10 narrative), `../banking-multisource-lakehouse-airflow-dag/journey/03_IDEMPOTENCY_D11.5.md` (the real gap→route→fix story behind the DIY below — read AFTER you've attempted the trace), `../banking-multisource-lakehouse-airflow-dag/dags/pipeline_dag.py`, `../banking-multisource-lakehouse-airflow-dag/include/PIPELINE_SIDE_CONTRACT.md` | trace one data-interval backfill end-to-end: name which watermark param crosses which repo boundary, and where `now()` would have silently broken it (journey/03 is the answer key — the extractor genuinely keyed `dt=` off `now()` until PR #15 fixed it) |

> **M10 artifacts live in the sibling repo** `banking-multisource-lakehouse-airflow-dag` — clone
> it alongside this one (`git clone .../banking-multisource-lakehouse-airflow-dag ../banking-multisource-lakehouse-airflow-dag`).
> If it isn't present at that path, STOP and surface it — don't teach M10 from memory of the ADR.

## How a module runs (the ritual)
1. @cikgu poses the WHY questions. You answer from reasoning — **NO reading yet.**
2. You sketch the solution shape.
3. THEN you open the artifact and compare to your reasoning.
4. For DIY modules: @cikgu writes a `learning/diy/TICKET_<name>.md`; you build in
   `learning/diy/`; diff vs the real model line-by-line; quiz WHY on every gap.
5. LEARNING_LOG entry + score update.

## Suggested order
M0 → M1 → M2 → M3 → M4 → M5 → M6 → M7 → M8 → M9 → M10.
(M4 MDM crosswalk is the spine of this project — no shared key across sources is THE defining
constraint; everything Gold depends on it. Do not skip ahead to M5 star schema before M4 clicks.)
