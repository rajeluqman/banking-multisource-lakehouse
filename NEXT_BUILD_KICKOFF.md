# Next Build Kickoff — decoupled orchestration + historical-data strategy

> Paste-able kickoff for a fresh Sonnet session (this repo's dedicated Codespace). Architecture
> for this round is DECIDED and gated green — this doc is execution only. Do NOT re-litigate
> `ADR-006`/`ADR-007`; if something in them looks wrong once you're reading real code, STOP and
> surface it, don't silently improvise (this repo's CLAUDE.md anti-shortcut rule).

## Read first, in this order
1. `governance/ADR/ADR-007-decoupled-orchestration-and-historical-data-strategy.md` — the design.
2. `governance/ADR/ADR-006-real-sap-hana-teradata-cdc-showcase.md` Addendum #1 — Teradata dual-role.
3. `journey/07_PIPELINE_SPEC.md` "Orchestration" + "Historical-data strategies" sections.
4. `BUILD_REPORT.md` — what's already built and what's UNVERIFIED (nothing here has been run
   against live data yet — no assumption of a working Bronze/Silver should be made blind).

## Task list (in dependency order — each item cites the ADR section that governs it)

1. **R-40 fix (ADR-007 D7.5) — initial-snapshot extractor for CDC sources.**
   Add a one-time full-read extraction to `seed/sap_hana/load_berka.py` and
   `seed/teradata/load_bank_marketing.py` (or a new `pipeline/extract/cdc_initial_snapshot.py`
   shared by both) that lands the just-seeded bulk data into Landing (same manifest/`_SUCCESS`
   shape as everything else) BEFORE the CDC pollers start. Without this, the seed data never
   reaches Bronze via the CDC path at all — confirm this by reading `pipeline/extract/
   cdc_common.py`'s `poll_cdc_log` and seeing it only reads `_cdc_log`, never the base table.

2. **Split `pipeline/silver/build_silver.py` into 5 domain pipelines (ADR-007 D7.1).**
   Create `silver_sales.py`, `silver_fraud.py`, `silver_crm.py`, `silver_marketing.py`,
   `silver_core_banking.py` — move the existing builder functions (`build_sil_application`,
   `build_sil_bureau`, `build_sil_card_txn`, `build_sil_client`, `build_sil_campaign_response`,
   the `SIMPLE_TABLES` passthrough loop) into the matching domain file. Shared helpers
   (`pipeline/silver/common.py`, `pipeline/silver/birth_number_decode.py`) stay shared — do NOT
   duplicate them per file. Delete `build_silver.py` once all builders have a new home; update
   any doc still referencing it by name (`gates/doc_reference_contract.py` will catch a miss).

3. **Config-driven orchestrator (ADR-007 D7.3).**
   Write `pipeline/orchestrate_config.yml` (dependency graph + per-source cadence: `batch` vs
   `cdc_poll` — see ADR-007 for the exact graph) and `pipeline/orchestrate.py` (reads the
   config, runs each stage after its upstream succeeds, writes a run-status row into the same
   control-plane store `pipeline/common/watermark.py` already uses). Follow the
   `gates/framework.yml` precedent: the script reads config, never hardcodes the DAG shape.

4. **`mart_pipeline_health.py` reads orchestrator run-status too (additive).**
   Extend the existing row-count reconciliation query to also surface the latest run-status
   per stage from step 3's control-plane store. Do not change the existing reconciliation logic.

5. **Partitioning fix (ADR-007 D7.4 Strategy 2).**
   Add `.partitionBy("txn_year", "txn_month")` to `pipeline/gold/fact_txn.py` and
   `fact_card_fraud.py` (derive the two columns from `txn_ts` via `year()`/`month()` before
   write). This is a real gap, not optional polish — confirm via `git log`/reading the current
   file that these writes are unpartitioned today.

6. **Explicit full-backfill flag (ADR-007 D7.4 Strategy 1).**
   Add a `--full-backfill` CLI flag to `pipeline/extract/postgres_extract.py` and
   `mssql_extract.py` that forces the full-pull branch in `jdbc_batch_common.py`'s
   `extract_table` regardless of existing watermark state (read, don't guess, the current
   watermark-or-full-pull logic before changing it).

7. **Teradata cold-tier SQL view (ADR-006 Addendum #1).**
   Write `pipeline/gold/cold_tier/teradata_cold_view.sql` — a native Teradata `CREATE VIEW`,
   AGGREGATE GRAIN ONLY (no `customer_id`, no row-level PII — this is a hard rule, not a style
   choice, see the ADR for why), filtering to rows dated before the CDC cutover. Document the
   cutover-date parameter clearly (it's per-deployment, not a constant).

## Gate before calling any of this done
```
python3 gates/journey_completeness.py
python3 gates/boundary_contract.py
python3 gates/doc_reference_contract.py
python3 gates/secrets_scan.py
python3 -m unittest discover tests
```
All four gates green + all unit tests passing is the bar — not "I wrote the files." If you can
run this against live Postgres/MSSQL/SAP HANA/Teradata in this Codespace, do so and capture real
output into `journey/08_SERVING_AND_EVIDENCE.md`; if you can't (no credentials yet), say so
explicitly and mark the relevant `BUILD_REPORT.md` rows UNVERIFIED — do not claim a live-run
proof you didn't actually produce.

## Update before ending the session
`PROJECT_STATUS.md` "▶ RESUME HERE" and `BUILD_REPORT.md` — same discipline as every prior fasa.
