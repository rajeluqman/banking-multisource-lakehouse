# banking-multisource-lakehouse — PROJECT STATUS (resume-safe checkpoint)

## ▶ RESUME HERE (read this first)
**All of Fasa 0 → D is built** (governance kit, 5-source seeding, Landing extractors + CDC,
promotion gate, Silver transforms, Gold star schema + all 10 BQ marts + UC RBAC grants). All
four bootstrap gates are green. Full self-audit: `BUILD_REPORT.md`.

**The single biggest remaining gap: nothing has been RUN.** No dataset downloaded, no Docker
container started, no Spark session executed, no live DB/cloud connection made — per owner
instruction, this entire build happened as code-only in the planning session. Every mart,
extractor, and gate is `py_compile`-clean and reviewed, but UNVERIFIED against live data. The
next session (in the owner's dedicated Codespace) should: provision SAP HANA Cloud + Teradata,
supply Kaggle credentials (or accept UCI-only partial data), run `make seed-all`, run the
extractors, run `pipeline/silver/build_silver.py` and each `pipeline/gold/*.py`, and capture
real query output into `journey/08_SERVING_AND_EVIDENCE.md`.

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
| gates/doc_reference_contract.py | ✅ OK — 20 docs, all references resolve | 2026-07-06 |
| gates/secrets_scan.py | ✅ OK (2 real hits caught + resolved mid-session, R-35) | 2026-07-06 |

## Open decisions for owner
- Provide Kaggle API credentials (`~/.kaggle/kaggle.json` or `KAGGLE_USERNAME`/`KAGGLE_KEY`) so
  Fasa A can seed from the REAL Home Credit / PaySim CSVs (Berka now sources via SAP HANA Cloud,
  UCI Bank Marketing needs no auth), OR confirm a synthetic schema-accurate placeholder is
  acceptable for the dev-loop and defer real data to later.
- Provision SAP HANA Cloud (BTP Free Tier) and Teradata (Vantage Express or Teradata Cloud free
  tier), enable internet-facing endpoints, and supply connection details via `.env` — required
  before Fasa B's CDC extractors can run live (code is written either way; live testing is
  UNVERIFIED until then).
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
