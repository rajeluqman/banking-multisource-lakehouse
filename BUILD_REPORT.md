# BUILD_REPORT.md — self-audit (Sonnet build session, 2026-07-05/06)

> Per `05_BUILD_AND_VERIFY_PROMPTS.md` prompt A. Written honestly: anything not run against
> live infrastructure this session is marked **UNVERIFIED**, not "done" — no live Spark, no
> live database/cloud connections were used in this planning session (owner instruction);
> that execution happens in the owner's dedicated Codespace.

## 1. Risk register (R-01…R-39) — where each is handled

| ID | Handled where (file:line / mechanism) | Status |
|---|---|---|
| R-01 | seed/postgres/load_home_credit.py (adds created_at/updated_at, D-03.1) | code done, UNVERIFIED live |
| R-02 | pipeline/silver/build_silver.py `build_sil_application` KEEP_COLUMNS prunes; Bronze keeps all verbatim | code done, UNVERIFIED live |
| R-03 | pipeline/silver/common.py `orphan_quarantine`, used by `build_sil_bureau` | code done, UNVERIFIED live |
| R-04 | journey/06_DQ_PLAN.md documents per-column null-rate expectations | **documented only — no executable null-rate check written yet** |
| R-05 | seed/postgres/load_home_credit.py `--sample` flag (D-14) | code done, UNVERIFIED live |
| R-06 | seed/common/seeding_utils.py `paysim_step_to_timestamp`, used in seed/mssql/load_paysim.py | code done, UNVERIFIED live |
| R-07 | seed/build_xwalk.py assigns customer_id to PaySim identifiers | code done, UNVERIFIED live |
| R-08 | pipeline/silver/build_silver.py `build_sil_card_txn` renames distinctly; pipeline/gold/fact_card_fraud.py reads ONLY `is_fraud` | code done, UNVERIFIED live |
| R-09 | seed/build_xwalk.py filters PaySim `M`-prefixed (merchant) identifiers out of the crosswalk — **verified this session** against synthetic fixtures (25/30 raw identifiers correctly filtered) | **verified (fixture test)** |
| R-10 | pipeline/extract/mssql_extract.py watermark-batch, not full SELECT * | code done, UNVERIFIED live |
| R-11 | journey/06_DQ_PLAN.md documents WARN-only acceptance | **documented only — no executable reconciliation check written yet** |
| R-12 | pipeline/silver/birth_number_decode.py — **verified this session, 7/7 unit tests pass** (tests/test_birth_number_decode.py) | **verified (unit test)** |
| R-13 | seed/sap_hana/load_berka.py `rebase_dates()` on account/card/loan/trans date columns | code done, UNVERIFIED live |
| R-14 | Currency tagged at seed (all loaders); **Gold-level static FX normalization table NOT built** | **partial — currency tagging done, FX conversion not implemented** |
| R-15 | Superseded — Berka moved off file-drop onto SAP HANA Cloud CDC (ADR-006); no file-drop source remains | N/A (superseded) |
| R-16 | pipeline/promote/promotion_gate.py `_check_schema_drift` (generic, platform-agnostic) | code done, UNVERIFIED live |
| R-17 | journey/06_DQ_PLAN.md documents a UTF-8 validity check | **documented only — no executable check written yet** |
| R-18 | pipeline/extract/obp_client.py `OBPClient._request` refreshes token on 401 | code done, UNVERIFIED live |
| R-19 | pipeline/extract/obp_client.py stores response JSON verbatim, no flattening at Landing | code done, UNVERIFIED live |
| R-20 | pipeline/extract/obp_client.py retry+backoff, circuit-break after MAX_RETRIES | code done, UNVERIFIED live |
| R-21 | journey/01_DATASET_AND_SOURCES.md documents snapshot-append treatment | documented, no special code needed |
| R-22 | pipeline/extract/obp_client.py `paginate_to_exhaustion` + `reconciled` flag; promotion_gate.py `_check_pagination_reconciled` | code done, UNVERIFIED live |
| R-23 | seed/build_xwalk.py — the MDM keystone; **verified this session** (fixture test showed 9/41 customers spanning >1 source) | **verified (fixture test)** |
| R-24 | pipeline/gold/dim_customer.py survivorship (priority-ranked window function) | code done, UNVERIFIED live |
| R-25 | Named limitation, ADR-004; drip_feed.py implements D-06 soft-delete | code done (mitigation), gap itself is by-design |
| R-26 | pipeline/extract/jdbc_batch_common.py `OVERLAP_WINDOW = 5 min` | code done, UNVERIFIED live |
| R-27 | pipeline/gold/grants/uc_grants.sql — Landing/Bronze REVOKE from all non-`pipeline_svc`/`landing_admin` roles | code done, UNVERIFIED against a live UC workspace |
| R-28 | Same as R-16 (shared gate) | code done, UNVERIFIED live |
| R-29 | **NOT implemented** — no unknown-member (-1) surrogate key logic written in any Gold builder | **gap — not yet implemented** |
| R-30 | pipeline/gold/mart_pipeline_health.py | code done, UNVERIFIED live |
| R-31 | pipeline/gold/grants/uc_grants.sql explicit REVOKE statements (defense-in-depth) | code done, UNVERIFIED against a live UC workspace |
| R-32 | uc_grants.sql — `pipeline_svc`/`serving_ro`/`landing_admin` are distinct roles, never shared | code done, UNVERIFIED live |
| R-33 | Platform-native (Unity Catalog query history/lineage) — no bespoke code by design | N/A by design |
| R-34 | `dim_customer_xwalk` makes cross-source erasure conceptually tractable; **no runnable erasure script exists** | **capability present, not built as a runnable tool** |
| R-35 | gates/secrets_scan.py — **caught 2 real false-positive-shaped matches this session** (drip_feed.py/seed loaders' f-string connection strings, obp_client.py's DirectLogin header) and required explicit `secrets-scan:allow` justification for each | **verified (gate actually fired and was resolved this session)** |
| R-36 | pipeline/promote/promotion_gate.py `_promote_cdc` anti-join on (pk_value, op, seq) | code done, UNVERIFIED live |
| R-37 | Same mechanism as R-36 (shared `_promote_cdc`) | code done, UNVERIFIED live |
| R-38 | seed/teradata/load_bank_marketing.py deterministic sampled linkage; **verified this session** via build_xwalk.py fixture test pattern (same sampling primitive) | code done, UNVERIFIED live (linkage-specific script not fixture-tested, only the shared xwalk logic was) |
| R-39 | journey/07_PIPELINE_SPEC.md documents the network-exposure prerequisite | documented, operational (owner action) |

**4 real gaps** (R-04, R-11, R-17, R-29) and **1 partial** (R-14) are honestly marked above,
not hidden. R-34 has the underlying capability but no packaged script.

## 2. Business questions (BQ-01…BQ-10)

All 10 have a Gold mart built this session (see `journey/08_SERVING_AND_EVIDENCE.md` for the
full table). **Every mart is code-complete and py_compile-clean, but UNVERIFIED against live
data** — no Spark session, no live Bronze/Silver data existed to run them against in this
planning session (owner instruction: no cloud/DB connections here). Definition of done
("one runnable query per BQ with captured output") is **NOT YET MET** — that is the
single largest remaining item, tracked in `PROJECT_STATUS.md`.

BQ-09 specifically has no dedicated mart file (by design — it's a direct query against
`fact_txn` × `dim_customer`, per `journey/02_BUSINESS_QUESTIONS.md`); that query itself has
not yet been written as a runnable script.

## 3. Journey completeness

All 9 journey docs present and filled (no `FRAMEWORK_TEMPLATE: UNFILLED` sentinel remaining).
`journey/09_SECURITY_AND_ACCESS.md` filled richly per D-16, not N/A'd.

```
$ python3 gates/journey_completeness.py
✅ journey completeness OK
```

## 4. Gate status (all four, run at the end of this session)

```
$ python3 gates/journey_completeness.py
✅ journey completeness OK
$ python3 gates/boundary_contract.py
✅ boundary contract OK
$ python3 gates/doc_reference_contract.py
DOC-REFERENCE CONTRACT: OK — 20 doc(s), all references resolve.
$ python3 gates/secrets_scan.py
✅ secrets scan OK
```

A real drift was caught and fixed mid-session: the moment the first Gold-layer file
(`pipeline/gold/dim_customer_xwalk.py`) existed, `doc_reference_contract.py`'s C1 check
activated and found `sil_customer_xwalk` vs `dim_customer_xwalk` naming inconsistency across
docs — reconciled to `dim_customer_xwalk` (see commit `7c443c6`). This is exactly the kind
of drift the gate exists to catch, and it worked.

## 5. Idempotency + reconciliation proofs (Fasa B/D gates)

**UNVERIFIED — not run.** The kickoff's Fasa B gate requires (a) kill-and-rerun an extractor
mid-run with no dupes/gaps, and (b) feed a partial/duplicate arrival and prove it's
quarantined with Bronze unchanged. Neither was executed — both require a live Spark session
against Postgres/MSSQL/SAP HANA/Teradata, which this planning session deliberately did not
start (owner instruction). The promotion gate's logic (`pipeline/promote/promotion_gate.py`)
implements both properties by design (manifest+`_SUCCESS` check, CDC anti-join dedup,
schema-drift firewall) but this is a code-review claim, not a run-time proof. **This is the
next concrete verification step for the owner's dedicated Codespace.**

## 6. Portability check

```
$ grep -rn "^import dlt\|^from dlt\|dbutils\." pipeline/ seed/
CLEAN: no DLT or dbutils usage found
```
`gates/boundary_contract.py` additionally enforces this as a standing gate (`banned_imports.dlt`),
so this isn't just a one-time grep — it's checked on every future commit too.

## 7. Security (D-16)

- `journey/09_SECURITY_AND_ACCESS.md`: filled richly, not N/A — verified by reading the file
  this session (§1–9 all substantive).
- RBAC matrix: real UC `GRANT`/`REVOKE` statements in `pipeline/gold/grants/uc_grants.sql`, not
  prose — includes explicit `REVOKE ALL PRIVILEGES ON SCHEMA banking.landing/bronze FROM
  analyst_marketing, fraud_ops, risk, serving_ro` as defense-in-depth (R-31).
- Secrets scan: green (see §4); 2 real hits were caught and resolved during this session
  (§1, R-35) — the gate demonstrably works, not just configured.
- No unmasked PII in Gold: **by design** (masking happens once at Silver, `pipeline/silver/
  common.py` `mask_last4` + `birth_number_decode.py` drop-after-decode; Gold only reads
  already-masked Silver columns) — **not runtime-verified** against live data this session.
- R-31…R-39: see §1 table — all handled or explicitly gapped, none silently skipped.
- No separate `security/` folder created — confirmed (`ls governance/` shows no `security/`
  directory; content lives entirely in `journey/09_SECURITY_AND_ACCESS.md`, per D-16/kit
  ADR-001 rej-alt #2).

## 8. What's NOT done (honest list, not buried)

1. **No live execution anywhere** — no dataset downloaded, no Docker container started, no
   Spark session run, no SAP HANA/Teradata/Postgres/MSSQL/OBP connection made. Per owner
   instruction, all of Fasa A–D was written as code only.
2. **4 real DQ gaps** (R-04 null-rate checks, R-11 balance-reconciliation check, R-17
   encoding-validity check, R-29 late-arriving-dimension unknown-member key) are documented
   in `journey/06_DQ_PLAN.md` but have no executable implementation yet.
3. **R-14 Gold-level FX normalization** (static seed table, D-12) not built — currency is
   tagged at seed, not yet converted to one reporting currency at Gold.
4. **R-34 right-to-erasure** — capability exists via `dim_customer_xwalk`, no runnable script.
5. **Fasa E (Snowflake/DuckDB serving veneer)** — not started; explicitly optional
   (`02_SONNET_BUILD_KICKOFF.md`), deferred until Gold is verified against live data.
6. **BQ-09's query** — not written as a standalone runnable script (direct `fact_txn` ×
   `dim_customer` join, described but not saved as code).
7. **Kaggle "Czech bank financial dataset" slug** in `scripts/fetch_datasets.py` is
   (unverified) — flagged inline in the file; needs the owner to confirm the exact Kaggle
   dataset slug before running.

## 9. What IS verified (real evidence, not claims)

- `tests/test_birth_number_decode.py` — 7/7 pass (pure-Python, no I/O needed, R-12).
- `seed/build_xwalk.py` — exercised against synthetic fixture CSVs (not real datasets): 55
  rows, 41 unique customers, 9 spanning >1 source, merchant-row filtering (R-09) confirmed
  correct (25 of 30 raw PaySim identifiers kept, 5 `M`-prefixed correctly excluded).
- All 44 Python files across `pipeline/` and `seed/` are `py_compile`-clean.
- All four bootstrap gates green, including two real secrets-scan hits caught and resolved
  mid-session (R-35 — the gate is demonstrably not hollow).
- `gates/doc_reference_contract.py` caught a real naming-drift bug the moment the first Gold
  file existed (`sil_customer_xwalk` vs `dim_customer_xwalk`) — fixed, not ignored.

## 10. ADR-007 build (2026-07-06, third session same day)

All 7 tasks from `NEXT_BUILD_KICKOFF.md` implemented, code-only (no live DB/cloud/Spark this
session, same split as every prior fasa). Per-task evidence:

| # | Task | Where | Status |
|---|---|---|---|
| 1 | R-40 initial-snapshot extractor | `pipeline/extract/cdc_initial_snapshot.py`, wired into `seed/sap_hana/load_berka.py` + `seed/teradata/load_bank_marketing.py` | code done; **smoke-tested this session** with a synthetic pandas fixture (local disk only, no live DB) — parquet + manifest + `_SUCCESS` written correctly, second call correctly returned `None` via the idempotency watermark guard |
| 2 | Silver split into 5 domain pipelines | `pipeline/silver/silver_{sales,fraud,crm,marketing,core_banking}.py`; `build_silver.py` deleted; shared helpers (`build_simple_table`, `latest_state_from_cdc_log`) moved into `pipeline/silver/common.py` per ADR-007's "shared helpers stay shared" rule | code done, UNVERIFIED live |
| 3 | Config-driven orchestrator | `pipeline/orchestrate_config.yml` + `pipeline/orchestrate.py` (Kahn's-algorithm topological sort, `--only` for a targeted + its transitive deps, `--poll-seconds` for cadence-differentiated re-runs — added in the verify round, see below) | code done, UNVERIFIED live. The yml's `depends_on` graph is the REAL per-file dependency graph (derived by reading every Gold builder's actual `layer_path()` reads this session), not the ADR's simplified `[silver] -> [6 dims/facts] -> [9 marts]` block-diagram — e.g. `dim_customer_xwalk`/`dim_date` load from a seed-time CSV / self-contained calendar range and have NO upstream pipeline-stage dependency, which the block-diagram would have gotten wrong. Also required giving `pipeline/promote/promotion_gate.py` a `main()` (it had none before — it was a library of functions called with explicit args, not a standalone runnable stage), since ADR-007 D7.3's graph names it as one node |
| 4 | `mart_pipeline_health.py` reads orchestrator run-status | `pipeline/common/watermark.py` gained `write_run_status`/`read_run_status` (same control-plane store, new `_control/run_status/<stage>.json` key shape); `mart_pipeline_health.py` adds `orchestrator_stage`/`orchestrator_status`/`orchestrator_error` columns, additive only — existing row-count reconciliation logic untouched | code done, UNVERIFIED live |
| 5 | Partitioning fix | `.partitionBy("txn_year", "txn_month")` on `fact_txn.py`/`fact_card_fraud.py`, columns derived via `year()`/`month()` on `txn_ts` before write | code done, UNVERIFIED live |
| 6 | `--full-backfill` flag | `postgres_extract.py`/`mssql_extract.py`; `jdbc_batch_common.extract_table()` gained a `full_backfill` param that forces `last_watermark = None` regardless of stored state | code done, UNVERIFIED live. Deliberately parsed ONLY in each script's `__main__` guard, not inside `main()` — `main()` keeps a zero-arg signature so `pipeline/orchestrate.py`'s in-process `module.main()` calls can't accidentally have the orchestrator's own `sys.argv` parsed by the stage's argparse |
| 7 | Teradata cold-tier SQL view | `pipeline/gold/cold_tier/teradata_cold_view.sql` — aggregate-only (job/education/prior-campaign-outcome/week), no `customer_id`, no row-level PII; cutover date is an explicit `{{CDC_CUTOVER_DATE}}` placeholder (deliberately not derived — `bank_marketing` has no real per-row event timestamp besides seed-time `created_at`) | written, UNVERIFIED — no live Teradata instance to run the DDL against this session |

**One follow-on gap this surfaced, named rather than silently expanded into scope**: R-40's
initial-snapshot data lands in Bronze as a plain (non-`_cdc`) batch-shaped table, but
`silver_crm.py`/`silver_marketing.py` still only read the `_cdc` op-log Bronze tables (this
was true of the pre-ADR-007 code too — not a regression). Wiring the UNION is a follow-up, not
part of `NEXT_BUILD_KICKOFF.md`'s 7 items.

**Verifying-architect review (2026-07-06, same day) found one real defect in task 3, since
fixed** — see `governance/ADR/ADR-007-...md` Addendum #2 for the full account. `cadence`
(`batch`/`cdc_poll`/`on_upstream`) was read from the yml into each stage dict but
`orchestrate.py` never referenced it again — every stage ran exactly once per invocation
regardless of cadence, which is the literal thing D7.3 said the orchestrator must not do
("doesn't treat a continuous CDC poller the same as a once-nightly batch job"). Confirmed by
`grep -c cadence pipeline/orchestrate.py` finding zero hits outside comments/docstrings
before the fix. **Fixed**: `orchestrate.py` gained `--poll-seconds N` — after the first full
pass, only `cdc_poll`/`on_upstream` stages re-run on each tick, `batch`-cadence extraction
stages are not re-run by the loop. Verified two ways: (a) a pure-Python check against the
real `orchestrate_config.yml` confirming the 3 batch-cadence stages
(`postgres_extract`/`mssql_extract`/`obp_client`) are excluded from `poll_stages` while
topological order is preserved; (b) a mocked-module run (no live Spark/DB) proving a fake
`batch` stage ran once across 1 full pass + 2 poll ticks while a fake `cdc_poll` stage ran 3
times and a fake `on_upstream` dependent of both ran 3 times without being falsely blocked.
All four gates + unit tests re-run green after the fix.

**Gate run (this session, after all 7 tasks)**:
```
$ python3 gates/journey_completeness.py
✅ journey completeness OK
$ python3 gates/boundary_contract.py
✅ boundary contract OK
$ python3 gates/doc_reference_contract.py
DOC-REFERENCE CONTRACT: OK — 21 doc(s), all references resolve.
$ python3 gates/secrets_scan.py
✅ secrets scan OK
$ python3 -m unittest discover tests
Ran 7 tests in 0.000s — OK
$ find pipeline seed tests -name "*.py" | xargs -n1 python3 -m py_compile
(all clean, including every new/changed file this session)
```
`doc_reference_contract.py`'s C2 check would have caught `ADR-007`'s own backtick references
to the now-deleted `build_silver.py` — fixed by de-backticking those 3 references (they're
historical-narrative prose describing a completed deletion, not a live path claim) rather than
leaving a dangling reference (same "drift is a lie waiting to mislead" discipline as Fasa D's
`sil_customer_xwalk`/`dim_customer_xwalk` catch).

## Hand-off

Per `05_BUILD_AND_VERIFY_PROMPTS.md`, this repo is ready for the Opus verify pass (prompt B) —
with the understanding that "verify against ground truth" for items in §8 and §10 above will
correctly find UNVERIFIED-live items still unverified against live infra, because they ARE,
and are named here rather than hidden. The owner's next action: provision SAP HANA Cloud +
Teradata, supply Kaggle credentials (or accept the UCI-only partial dataset set), open a
dedicated Codespace, and run Fasa A → D plus the new orchestrator for real — at which point
§5's idempotency proofs, §2's per-BQ query outputs, and §10's UNVERIFIED-live rows all become
obtainable.
