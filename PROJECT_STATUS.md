# banking-multisource-lakehouse — PROJECT STATUS (resume-safe checkpoint)

## ▶ RESUME HERE (read this first)

**2026-07-14 — LATEST (front of queue): source #4 swapped SAP HANA Cloud → Salesforce**
(Developer Edition; still the CRM role; Berka stays the seeded data + golden-record keystone,
ADR-005 L26). Driver: SAP BTP signup blocked by a mobile-OTP wall; Salesforce Dev Edition is
free/email-verified, the #1 CRM, and adds a genuinely new SaaS-API-ingestion skill. Ingestion =
Salesforce **Bulk API 2.0 + `SystemModstamp` incremental** INTO the medallion (federated
direct-query was verified an anti-pattern and rejected). BQ-03's CRM-ticket gap is now filled by
Salesforce **Case** (enrichment, not an 11th BQ); scope-guardian sent two tempting use cases
(address-velocity, complaint-pattern) to BACKLOG and accepted disciplined-payer cross-sell as
BQ-05/06 enrichment. The `architect` agent was **merged into `staff-data-engineer`** (single top
technical authority). Full design: `governance/ADR/ADR-006-...md` **Addendum #2**. **Architecture +
design + scope are COMPLETE and all 4 gates green — the NEXT job is the BUILD (Fasa A→D live),
see `NEXT_BUILD_KICKOFF.md`.** ⚠ Pipeline CODE still uses the internal source key `sap_hana` in
~12 files — the build must rename it to `salesforce` (ADR-006 Add #2 "Internal source-identifier
key"). Everything below this block predates the swap; where older entries say "SAP HANA", the
current source #4 is Salesforce.

**Fasa 0 → D is built, ADR-007's 7-task build is code-complete, AND the verifying-architect
review round is closed** (2026-07-06, fourth session same day). The architect review (ULTIMATE
VETO) found one real defect — `orchestrate.py` read `cadence` off each stage but never acted
on it, so a continuous CDC poller and a once-nightly batch job were treated identically,
exactly what ADR-007 D7.3 said must not happen. **Fixed**: `orchestrate.py` now supports
`--poll-seconds N`, which re-runs only `cdc_poll`/`on_upstream` stages on each tick while
`batch`-cadence extraction stages stay one-shot — verified both against the real
`orchestrate_config.yml` (topological order preserved after filtering) and via a mocked-module
run (no live Spark/DB) proving the differentiated re-run counts. Full account:
`governance/ADR/ADR-007-...md` Addendum #2, `BUILD_REPORT.md` §10. All four gates + the full
unit-test suite are green after the fix — see `BUILD_REPORT.md` §"ADR-007 build (2026-07-06)"
for the per-task evidence. **Nothing has been run against live infrastructure yet** (no Spark,
no live DB/cloud connections — owner instruction, this is still the shared planning Codespace)
— that is the next session's job, in the owner's dedicated Codespace: provision Salesforce
(Developer Edition) + Teradata, supply Kaggle credentials (or accept UCI-only partial data), run Fasa A → D for
real plus the orchestrator (including a real `--poll-seconds` run against live CDC pollers),
THEN capture real output into `journey/08_SERVING_AND_EVIDENCE.md`.

One follow-on gap surfaced by this session's R-40 work, not part of ADR-007's task list and
therefore NOT implemented here (documented, not silently expanded into scope): the R-40
initial-snapshot extractor lands the seed-time bulk load into Bronze as a plain (non-`_cdc`)
batch-shaped table, but `pipeline/silver/silver_crm.py`/`silver_marketing.py` only read the
`_cdc` op-log Bronze tables — so that snapshot data doesn't reach Silver/Gold yet. A future
task should UNION the initial-snapshot Bronze table into those two domain pipelines' latest-
state read.

**2026-07-10 — R-41 named (documented only, not built, owner's explicit choice this session):**
no Delta `OPTIMIZE`/Z-ORDER compaction step exists anywhere in the pipeline. The CDC-poll
pattern (`pipeline/extract/cdc_common.py`, ADR-006 D6.3) plus the promotion gate's per-poll
append to Bronze (`pipeline/promote/promotion_gate.py`) will accumulate many small Delta
partitions over time — the classic small-files problem, slowing `pipeline/silver/common.py`'s
MERGE reads and eventually Snowflake/DirectQuery reads over Gold. No new ADR needed to close
this (Delta already supports `OPTIMIZE`/`ZORDER BY` natively per ADR-002) — just an unbuilt
maintenance stage, likely a new `compact` cadence in `pipeline/orchestrate_config.yml`
(ADR-007's stage model) when it's prioritized. Full detail: `journey/06_DQ_PLAN.md` "Known
accepted quality gaps" table, R-41.

**2026-07-06 (second architecture round, same day) — `ADR-007`**: owner asked for the pipeline
to look decoupled/fault-isolated like a real bank's estate (many small pipelines per layer, not
one script) and raised 3 historical-data problems (initial-load-vs-incremental, partition
pruning, hot/cold economics). Resulted in: `ADR-007` (Silver splits into 5 domain pipelines,
config-driven orchestrator, partitioning fix, explicit full-backfill flag) + `ADR-006` Addendum
#1 (Teradata dual-role — CDC source AND a native cold-tier SQL view Power BI DirectQueries,
bypassing the medallion for pre-cutover AGGREGATE-ONLY history). Also surfaced a real gap
(**R-40**): the CDC extractors never capture the seed-time bulk load (triggers only fire on
CHANGES after install) — needed an initial-snapshot extraction step, now built (see above).

Original blocker (still relevant context): this build environment has no Kaggle API
credentials and no live AWS/Databricks/Snowflake credentials, so Home Credit/PaySim/Berka
aren't obtainable here — surfaced to the owner rather than silently worked around
(anti-shortcut/STOP-GATE rule).

**2026-07-06 owner override (ADR-006):** mid-build, the owner reopened the source architecture —
replaced the SAP-sim file-drop simulation with a real **SAP HANA Cloud** (BTP Free Tier) instance,
and added **Teradata** (UCI Bank Marketing dataset) as a 5th source — both specifically to build
real CDC-connector extraction (portable trigger + change-table pattern, not platform-native SAP
SLT/Teradata QueryGrid). Trial-wall risk is explicitly accepted as a non-issue for the owner's
operating model (same reasoning already accepted for the Databricks trial, ADR-002). All governing
docs (journey 01–07, 09, `gates/framework.yml`, `governance/BOUNDARY_CONTRACT.md`, `BACKLOG.md`,
`CLAUDE.md`) have been updated to reflect 5 sources; `governance/ADR/ADR-006-...md` is the design
of record. **The owner has also directed that no dataset downloads or heavy compute (Docker,
SAP HANA/Teradata connections) happen in this planning session — those run in a dedicated
Codespace the owner will open separately.** This session writes code only.

See `BUILD_REPORT.md` for the full resolution path taken (what was built anyway, what's blocked,
what the owner needs to supply — including SAP HANA Cloud/Teradata provisioning + connection
details, never pasted into chat).

## Journey doc status
| Doc | Status |
|---|---|
| journey/01_DATASET_AND_SOURCES.md | done |
| journey/02_BUSINESS_QUESTIONS.md | done |
| journey/03_DATA_REQUIREMENTS.md | done |
| journey/04_DATA_MODEL.md | done |
| journey/05_STTM.md | done |
| journey/06_DQ_PLAN.md | done |
| journey/07_PIPELINE_SPEC.md | done |
| journey/08_SERVING_AND_EVIDENCE.md | done (contract only — per-BQ evidence rows filled at Fasa D) |
| journey/09_SECURITY_AND_ACCESS.md | done, filled richly per D-16 |

## Gate status (last run)
| Gate | Result | Date |
|---|---|---|
| gates/journey_completeness.py | ✅ OK | 2026-07-06 |
| gates/boundary_contract.py | ✅ OK | 2026-07-06 |
| gates/doc_reference_contract.py | ✅ OK — 21 docs, all references resolve | 2026-07-06 |
| gates/secrets_scan.py | ✅ OK (2 real hits caught + resolved mid-session, R-35) | 2026-07-06 |
| python3 -m unittest discover tests | ✅ OK — 7/7 pass | 2026-07-06 |

## Open decisions for owner
- Provide Kaggle API credentials (`~/.kaggle/kaggle.json` or `KAGGLE_USERNAME`/`KAGGLE_KEY`) so
  Fasa A can seed from the REAL Home Credit / PaySim CSVs (Berka now sources via Salesforce,
  UCI Bank Marketing needs no auth), OR confirm a synthetic schema-accurate placeholder is
  acceptable for the dev-loop and defer real data to later.
- Provision Salesforce (Developer Edition — free, email-verified; set up a Connected App for
  OAuth + reset the security token) and Teradata (Vantage Express or Teradata Cloud free tier),
  and supply connection details via `.env` (`SALESFORCE_*`, `TERADATA_*`) — required before
  Fasa B's extractors can run live (code is written either way; live testing is UNVERIFIED until
  then).
- Confirm the S3 bucket/prefix (`s3://<bucket>/banking/`) and whether AWS credentials will be
  supplied for a real S3-backed dev loop, or whether local-disk fallback is acceptable until the
  canonical Databricks-trial run.
- Confirm timing for the disposable Databricks trial (D-01 Add #3) and any live Snowflake account
  (Fasa E) — neither is needed until Fasa D Gold exists.

## Session log
- 2026-07-05: Fasa 0 bootstrap complete — framework kit copied and filled (framework.yml, journey
  01–09, 7 agents incl. cikgu, ADR-000/001 from kit + project ADR-002…005, CI workflow, CLAUDE.md,
  this file). All four bootstrap gates green. Data/credential blocker identified before Fasa A —
  surfaced to owner per STOP-GATE rule rather than silently substituting fake data as real.
- 2026-07-06: Owner override (ADR-006) — 5-source architecture (added SAP HANA Cloud replacing
  SAP-sim, added Teradata/UCI Bank Marketing), CDC-poll extraction pattern for both, BQ-01/05/06
  enrichment. All journey docs + governance updated in this same session; dataset downloads and
  container/cloud connections deliberately NOT run in this session per owner instruction (reserved
  for a dedicated Codespace).
- 2026-07-06: Fasa A (seed loaders, xwalk, drip-feed, CDC DDL), Fasa B (Landing extractors incl.
  CDC, promotion gate), Fasa C (Silver transforms, birth_number decode unit-tested 7/7 pass),
  Fasa D (Gold star schema, all 10 BQ marts, UC RBAC grants) all built and committed. Full
  self-audit in `BUILD_REPORT.md` — 4 real DQ gaps (R-04/R-11/R-17/R-29) and the "nothing has
  been run against live data" limitation are named explicitly, not hidden.
- 2026-07-06 (third session, same day): ADR-007's all 7 tasks implemented — R-40 initial-
  snapshot extractor (`pipeline/extract/cdc_initial_snapshot.py`, smoke-tested locally with a
  synthetic fixture — parquet + manifest + `_SUCCESS` written correctly, idempotency guard
  confirmed); Silver split into 5 domain pipelines (`build_silver.py` deleted); config-driven
  orchestrator (`pipeline/orchestrate_config.yml` + `orchestrate.py`, real per-file dependency
  graph, not the ADR's simplified block-diagram — see the yml's header comment for why);
  `mart_pipeline_health.py` additively reads orchestrator run-status; `fact_txn`/
  `fact_card_fraud` partitioned by `txn_year`/`txn_month`; `--full-backfill` flag on
  `postgres_extract.py`/`mssql_extract.py` (designed so `orchestrate.py`'s in-process
  `module.main()` calls can't be broken by argparse reading the orchestrator's own argv — see
  each file's `main()` docstring); `pipeline/gold/cold_tier/teradata_cold_view.sql`
  (aggregate-only, cutover date is an explicit per-deployment placeholder, not derived). All
  four gates + `python3 -m unittest discover tests` (7/7) green; every touched/new `.py` file
  py_compile-clean. One follow-on gap surfaced and documented above (initial-snapshot Bronze
  data not yet UNIONed into Silver) rather than silently expanded into this session's scope.
- 2026-07-06 (fourth session, same day) — verifying-architect review (ULTIMATE VETO): ran the
  actual gate bar rather than trusting the prior session's claim (confirmed green), traced
  R-40/partitioning/full-backfill/cold-tier view against ground truth (all confirmed correct),
  and caught one real defect — `orchestrate.py` never used the `cadence` field it read from
  `orchestrate_config.yml`, so every stage ran identically regardless of batch/cdc_poll/
  on_upstream, contradicting ADR-007 D7.3's stated reason for the field existing. **Fixed same
  session**: `orchestrate.py` gained `--poll-seconds N` (only `cdc_poll`/`on_upstream` stages
  re-run per tick, `batch` stages stay one-shot) — verified against the real config (order
  preserved after cadence filtering) and via a mocked-module run (differentiated re-run counts
  confirmed: batch=1, cdc_poll=3, on_upstream-dependent=3 across 1 pass + 2 ticks). All four
  gates + unit tests re-confirmed green after the fix. Full account: `governance/ADR/ADR-007-
  ...md` Addendum #2, `BUILD_REPORT.md` §10.
