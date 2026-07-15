# banking-multisource-lakehouse — PROJECT STATUS (resume-safe checkpoint)

## ▶ RESUME HERE (read this first)

**2026-07-15 (fourth session, later same day) — LATEST: R-14/D-12 CURRENCY NORMALIZATION BUILT —
a real, live correctness bug in marts already marked PROVEN is now fixed. Full detail:
`BUILD_REPORT.md` §16, `journey/08_SERVING_AND_EVIDENCE.md` (BQ-01/BQ-06/BQ-08 evidence lines
updated with corrected numbers).** Summary:

- D-12 ("Gold normalizes to MYR via a static FX seed table") and R-14 (its blocking DQ gate) were
  documented since the planning lab but never built. `mart_daily_flows.py`/`mart_customer_360.py`
  were silently summing Berka's CZK legs and PaySim's MYR legs together with zero conversion —
  confirmed live via `CUST_BK_1179`, whose `431259.62` `total_txn_value` was already sitting in
  `journey/08_SERVING_AND_EVIDENCE.md` as "real" evidence; corrected to `413972.663`.
- Got `@staff-data-engineer` sign-off (STOP-GATE, Gold model/schema territory) before building:
  new conformed dimension `dim_fx_rate` (`seed/artifacts/fx_rates.csv` +
  `pipeline/gold/dim_fx_rate.py`, ADR-005 addendum #1), FX conversion done ONCE at the fact grain
  via `to_myr` (`pipeline/gold/common.py`) — additive `amount_myr`/`current_balance_myr`, native
  `amount`/`currency` columns kept for lineage.
- **Scope conflict surfaced and escalated, not silently resolved**: the sign-off's design needed
  a real `currency` column on 3 existing Silver tables (`sil_trans`, `sil_application`,
  `sil_campaign_response`), but this session's brief said Silver/Bronze are untouched
  ("Gold-layer-only"). The permission system blocked the first attempt at deleting/rebuilding
  those Silver tables; asked the owner directly, approved. No live source connections used — all
  3 tables rebuilt locally from Bronze data already on disk.
- `pipeline/gold/dq_currency_gate.py` (R-14) now passes for real against 6 monetary columns
  across all 5 sources — wired into `pipeline/orchestrate_config.yml` ahead of
  `fact_txn`/`fact_card_fraud`. `AMT_INCOME_TOTAL` (Home Credit) is a documented D-12 exception
  (`unitless`, never converted — anonymized data, real currency unknown, never summed cross-
  source).
- Real before/after evidence (row counts unchanged — value-correctness fix, not a grain/join
  fix): `mart_customer_360.CUST_BK_1179` `431259.62`→`413972.663`; `mart_cross_sell.CUST_397288`
  `59361.9`→`12169.1895`; `mart_daily_flows.total_deposits_snapshot` `6255598.0`→`1282397.59`
  (the largest correction, a 79.5% overstatement). Audited every other Gold builder summing
  money (`mart_fraud_daily`, `mart_risk_segment`) — both confirmed single-currency, not buggy,
  left unchanged.
- Doc correction: `journey/05_STTM.md` previously said PaySim's `amount` currency was
  "unitless" — the actual seed code has always tagged `MYR`; corrected against live Bronze
  schema (map vs. territory, CLAUDE.md anti-shortcut rule #5).
- All 4 gates + `python3 -m unittest discover tests` (7/7) green.

**Next session**: no known blockers. Candidate next work: same as below (OBP mart wiring, Fasa E,
canonical Databricks trial) — none touched or changed by this session's fix.

**2026-07-15 (third session, later same day) — ALL 5 SOURCES LIVE, 10/10 BUSINESS
QUESTIONS PROVEN. Full detail: `journey/08_SERVING_AND_EVIDENCE.md`, `BUILD_REPORT.md` §15.**
Summary:

- Salesforce org setup was completed by the owner mid-session (live-reverified via `describe()`
  — 100% clean); Teradata's ClearScape environment was found live (resumed by the owner between
  sessions, confirmed via a real connection, not assumed) and brought in this session with
  explicit owner approval since it was originally out of scope; OBP was rewritten from scratch
  to pull real public-sandbox demo data instead of the always-empty `/my/accounts` endpoint
  (also owner-approved, since it wasn't in the original kickoff scope either).
- **14 real, previously-undetected bugs found and fixed** by actually running this pipeline
  end-to-end for the first time against all 5 sources — full list + fix rationale in
  `journey/08_SERVING_AND_EVIDENCE.md`. Two were serious enough to have silently corrupted every
  customer-level Gold number: `fact_txn`/`fact_card_fraud`'s PaySim legs joined a Silver column
  MASKED per D-07 against an UNMASKED crosswalk key (100% NULL `customer_id` for 20,000+ rows),
  and `seed/build_xwalk.py` sampled PaySim customer IDs from a completely different random draw
  than what was actually seeded into MSSQL (different RNG namespace — ~62/20,000 real overlap).
  Both fixed, reverified with 0 NULL `customer_id` and a 100% xwalk↔MSSQL overlap; required a
  full downstream rebuild (`dim_customer_xwalk` → `dim_customer` → `fact_txn`/`fact_card_fraud`
  → every dependent mart) plus a Teradata re-seed (its `customer_id` assignment used the old,
  broken xwalk).
- Salesforce also needed a real live-diagnosed fix beyond org setup: the Developer Edition org's
  5MB `DataStorageMB` cap silently rejected most of the previous session's `--sample 5000` load
  (Bulk API 2.0 returns HTTP success even on 100% server-side failure) — rearchitected
  `seed/salesforce/load_berka.py` to a small, ACCOUNT-rooted coordinated sample (150 accounts)
  instead of independently sampling every table, fixed real failure-reporting, and fixed a
  genuine Salesforce platform rule (a Contact needs a primary `AccountId` before any additional
  `AccountContactRelation` can be created for it) that wasn't a Setup-UI issue at all.
- Teradata's CDC DDL (`seed/common/cdc_ddl.py`) had never been run against real Teradata before
  this session — it was written against SAP HANA syntax (`CREATE COLUMN TABLE`, bare
  `GENERATED ALWAYS AS IDENTITY`, bare `BEGIN`, `:alias.col` references) and needed 4 separate
  live-verified syntax corrections before insert/update/delete triggers actually fired.
- **mart_pipeline_health now shows `reconciled=true` for all 5 sources — no Slack alert fired**,
  the first time this has been true for this project. All 4 gates + `unittest discover tests`
  green.
- OBP has real data now (20 accounts, 183 transactions from the public sandbox) but no Gold mart
  currently reads it — wiring it into a mart was out of scope this session (not requested,
  would need product/scope sign-off first).

**Next session**: no known blockers remain on any of the 5 sources. Candidate next work: decide
whether OBP's real public-sandbox data should feed a new/existing mart (scope decision, not a
technical blocker); Fasa E (Snowflake/Power BI serving veneer, optional); canonical Databricks
trial run for screenshot evidence (D-01 Add #3, still deferred — dev-loop local Spark is what
every session so far has actually exercised).

**2026-07-15 (second session, later same day) — LATEST: `NEXT_BUILD_KICKOFF.md` EXECUTED for
real. First actual Fasa A→D run against live infrastructure (Docker Postgres/MSSQL, real Kaggle
downloads, local Spark+Delta) — PARTIAL: Postgres/MSSQL/OBP live end-to-end through Gold;
Salesforce and Teradata genuinely blocked on owner-only actions (confirmed live, not assumed).
All 4 gates + `unittest discover tests` green. Full detail: `BUILD_REPORT.md` §14,
`journey/08_SERVING_AND_EVIDENCE.md`.** Summary:

- **Datasets downloaded for real** (`scripts/fetch_datasets.py`, after fixing 2 wrong/guessed
  Kaggle slugs — Berka's guessed slug didn't exist at all; Home Credit's was actually a
  *competition* slug needing rules accepted on kaggle.com, no API path, 401'd live). Real Home
  Credit (307,511 apps), PaySim (6.36M txns, 6.9M unique customer ids), Berka (account/card/
  client/disp/district/loan/order/trans), UCI Bank Marketing (45,211 rows) all on disk.
- **Docker Postgres 16 + MS SQL Server 2022 stood up and seeded** (`--sample 5000`/`--sample
  20000` per D-14 dev-loop scale — full Kaggle-scale data, e.g. PaySim's 6.36M rows or Home
  Credit's 13.6M-row `installments_payments`, is far past dev-loop scale). Fixed a real MSSQL
  container bootstrap failure (weak default password rejected by SQL Server's complexity
  policy) and a missing system ODBC driver (`msodbcsql18`, installed via apt).
- **Local Spark 3.5.3 + Delta 3.2.1 made to work for the first time ever in this repo** — this
  had literally never been run before (every prior session explicitly deferred to "the owner's
  dedicated Codespace"). Found and fixed: JDK 25 incompatibility (Spark's Hadoop client calls a
  removed Security Manager API — installed JDK 17 alongside, `JAVA_HOME`-scoped, system default
  untouched), missing Delta/Postgres/MSSQL JDBC jars in `pipeline/common/spark_session.py` (now
  resolved via Maven coordinates in local mode; `pyspark`/`delta-spark` added to
  `requirements.txt` — another real reproducibility gap, same class as the 2026-07-14 session's
  boto3/kaggle fixes).
- **Salesforce/Teradata confirmed still blocked, live, not assumed**: a real `describe()` audit
  showed the Salesforce org still lacks every custom object/field this build needs (unchanged
  from `BUILD_REPORT.md` §13); a real Teradata connection attempt timed out (ClearScape
  suspended, needs an owner dashboard resume, no API to do this). Both skipped per
  `NEXT_BUILD_KICKOFF.md`'s own explicit fallback instruction, not silently worked around.
- **8 real, previously-undetected bugs found and fixed** by actually running this pipeline for
  the first time (full list + evidence: `journey/08_SERVING_AND_EVIDENCE.md`) — most
  significant: **all 14 `pipeline/gold/*.py` builder modules were missing the `main() -> int`
  entrypoint** `pipeline/orchestrate.py`'s contract requires; every Gold stage would have
  crashed with `AttributeError` the first time the orchestrator ever tried to run one. Also
  fixed the pre-existing, previously-flagged R-30 defect (`mart_pipeline_health.py`'s Silver
  row-count path bug) — confirmed live and fixed, `postgres`/`mssql` now correctly show
  `reconciled=true`.
- **Real Gold output for BQ-02 (mart_fraud_daily), BQ-04 (mart_loan_funnel), BQ-10
  (mart_pipeline_health)** — actual command output pasted into `journey/08_SERVING_AND_
  EVIDENCE.md`, marked PROVEN, not just built. BQ-01/03/05/06/07/08/09 remain UNVERIFIED —
  blocked on `dim_customer`/`fact_txn`, which need Salesforce's `silver_crm` output.
- **A real Slack alert fired** from `mart_pipeline_health`'s reconciliation check (Salesforce/
  Teradata/OBP correctly flagged as unreconciled, no data) — owner explicitly chose to let it
  fire rather than suppress it, since it's an accurate signal, not a false alarm.

**Next session**: once the owner has done the Salesforce org setup (`BUILD_REPORT.md` §13's
checklist) and resumed the Teradata ClearScape environment, re-run `seed/salesforce/
load_berka.py` → `pipeline/extract/salesforce_extract.py` → `pipeline/silver/silver_crm.py` and
`seed/teradata/load_bank_marketing.py` → `pipeline/extract/teradata_extract.py` →
`pipeline/silver/silver_marketing.py`, then the 7 still-blocked Gold stages (`dim_customer`,
`fact_txn`, `mart_customer_360`, `mart_cross_sell`, `mart_daily_flows`, `mart_dormancy`,
`mart_risk_segment`, `mart_fraud_followup`) for a genuinely complete Fasa A→D proof.

**2026-07-15 (first session) — `NEXT_BUILD_KICKOFF.md`'s 6-task Salesforce-swap BUILD was
code-complete but NOT yet executed. All 4 gates + `unittest discover tests` green. Full detail:
`BUILD_REPORT.md` §13.** Summary:

- All 6 tasks done: source-key rename (`sap_hana`→`salesforce`, zero live-code hits left),
  `seed/salesforce/load_berka.py` (Bulk API 2.0 insert lifecycle + synthetic Case generation),
  `pipeline/extract/salesforce_extract.py` + new `pipeline/extract/salesforce_auth.py` (Client
  Credentials Flow — `simple_salesforce`'s own login doesn't support this grant type, verified
  by reading its source), `silver_crm.py` rewritten (6 builders) + `mart_fraud_followup.py`
  updated to consume the real Case timestamp, `orchestrate_config.yml`/`drip_feed.py` updated,
  `mart_pipeline_health.py` source map fixed.
- **Real gap surfaced mid-build, resolved WITH the owner (AskUserQuestion), not silently**:
  Task 2's 4-object Salesforce mapping had no home for Berka's `trans` (needed by `fact_txn.py`
  → BQ-01/BQ-06, P0) or `district` (R-03 orphan-check). Owner chose to add 2 new custom Salesforce
  objects (`Transaction__c`, `District__c`) rather than drop/redesign the P0 fact. `card`/`loan`
  dropped (unused downstream, disclosed).
- **A live describe()-based audit of the real org (not guesswork) found the actual live-org gap
  is LARGER than Task 2 assumed**: `AccountContactRelation` doesn't exist in this org at all
  (needs "Contacts to Multiple Accounts" enabled in Setup — an org toggle, not just a field);
  none of the new custom fields on Contact/Account exist yet; `Transaction__c`/`District__c`
  don't exist yet; `Case.Type` picklist needs 3 new values; `Case.CreatedDate` isn't API-settable
  without "Set Audit Fields upon Record Creation" enabled. Full checklist: `BUILD_REPORT.md` §13.
- **Consequence**: a live seed/extract run against Salesforce will fail today until the owner
  does the org setup above — not attempted (would be faking success). Postgres/MSSQL Docker
  still not started; Teradata still needs a ClearScape dashboard resume. `journey/08_SERVING_
  AND_EVIDENCE.md` NOT updated this session — no real Fasa A→D run exists yet to record.
- **Pre-existing bugs found this session, one fixed one flagged-not-fixed** (neither is new
  scope creep — both surfaced while rewriting/touching the exact same files for the swap): (1)
  FIXED — the old `silver_crm.py` masked `trans.account_id` but not `disp.account_id`, which
  would have silently broken `fact_txn.py`'s join; account_id is now unmasked everywhere as a
  join key, `trans.partner_account` masked instead. (2) NOT FIXED, flagged — `mart_pipeline_
  health.py`'s `_row_count` reads Silver via `layer_path("silver", source, table)` (adds a
  `source` segment) but `merge_upsert` actually writes Silver at `layer_path("silver", table)`
  (no source segment) — affects ALL 5 sources' `silver_row_count`/`reconciled` columns, a real
  BQ-10 (R-30) defect, out of scope for a source-swap task to silently redesign.

**Next session**: owner does the live-org setup listed in `BUILD_REPORT.md` §13, then re-run
`seed/salesforce/load_berka.py` → `salesforce_extract.py` → `pipeline/promote/promotion_gate.py`
→ `pipeline/silver/silver_crm.py` for a real Fasa A→D proof; also stand up Postgres/MSSQL Docker
and resume Teradata's ClearScape environment for the other two live sources; consider fixing the
`mart_pipeline_health.py` Silver-path bug (item 2 above) since it blocks BQ-10 reconciliation
being trustworthy for any source, not just Salesforce.

**2026-07-14 — ALL 8 credential/infra services provisioned + an Opus
verify pass re-confirmed 6/7 live; `ADR-002` Addendum #2 written (Databricks AWS→Azure switch);
`requirements.txt` reproducibility gap fixed. The `NEXT_BUILD_KICKOFF.md` code build (6 tasks)
still has NOT started.** Two sessions on 2026-07-14: a Sonnet setup session (batch 1 =
Salesforce/Teradata/OBP/Kaggle, then batch 2 = AWS/Slack/Snowflake/Databricks), then an Opus
verify/ADR session. The detailed batch-1 block is retained below; this block adds batch 2, the
Databricks decision, and the independent verify result.

- **Opus verify pass (independent re-run, not trusting the summary): 6/7 live-PASS.** Re-ran a
  consolidated smoke test hitting Salesforce, Teradata, OBP, Kaggle, AWS S3, Snowflake, Databricks
  (Slack skipped — a re-POST would spam the channel; its prior `200 OK` is unambiguous). Salesforce
  (custom fields still present), OBP, Kaggle, AWS S3 (write/read/delete round-trip), Snowflake,
  Databricks all PASS. **Teradata FAILED with a socket i/o-timeout — this is EXPECTED and benign:
  ClearScape free-tier environments auto-stop on idle (owner-confirmed). Credentials are correct
  (they connected earlier this session); the environment is merely suspended. ACTION before any
  next live run: resume the ClearScape environment in its dashboard first.**
- **AWS S3**: bucket `banking-lakehouse-pipeline`, IAM user access key in `.env`. Full
  read/write/delete round-trip verified via `boto3` (and again cross-cloud from the Databricks
  cluster). This is the real `s3://<bucket>/banking/` sole-source-of-truth from ADR-002; the
  local-disk fallback in `pipeline/common/lake_paths.py` is now optional, not forced.
- **Slack**: `SLACK_WEBHOOK_URL` filled; a test POST returned `200 ok` and landed a real message
  (the failure-alert path in `journey/07_PIPELINE_SPEC.md` "Failure handling").
- **Snowflake**: free trial (Standard, **AWS AP_SOUTHEAST_5** region — same-cloud as the S3 bucket,
  good for future external tables). Connected via `snowflake-connector-python`; `SELECT
  CURRENT_VERSION()` → `10.24.101`. Fasa E / serving only — not needed until Gold exists.
- **Databricks → AZURE, not AWS (major decision, now `ADR-002` Add #2).** AWS-hosted Databricks was
  attempted first and blocked twice on the owner's account (instant trial gives SQL-warehouse-only
  compute, cannot run PySpark; "connect-your-own-AWS"/Marketplace both hit *"free plan not eligible
  to purchase paid offers"*). Switched to **Azure Databricks** (Premium tier, isolated Resource
  Group, single-node cluster, 20-min auto-terminate). UC metastore auto-attached; default catalog
  `banking_lakehouse_dbx` visible. **Cross-cloud S3 read+write verified live from the cluster.**
  KEY LIMITATION, documented in `ADR-002` Add #2: **Unity Catalog on Azure Databricks can only
  register an AWS S3 external location READ-ONLY** (hard Microsoft-documented platform limit, not a
  trial/config issue). So the pipeline's S3 writes use **cluster-level Spark/boto3 creds** (AWS keys
  as cluster env vars), NOT UC-governed — i.e. Gold's "Unity Catalog governed" property does not
  hold for the S3 data path under this host. Named gap, not hidden. S3-as-truth + Snowflake serving
  story unaffected.
  - ⚠ **Cost note**: the Databricks cluster was still `RUNNING` at verify time. 20-min idle
    auto-terminate is set, but the owner can stop it manually in the workspace (or ask an assistant
    with `DATABRICKS_HOST`/`DATABRICKS_TOKEN` to stop it) to conserve trial credit between sessions.
- **`requirements.txt` fixed (was a reproducibility gap):** added `simple-salesforce`, `boto3`,
  `snowflake-connector-python`, `databricks-sdk`, `kaggle` (all pip-installed ad-hoc during setup,
  none were recorded — a fresh environment couldn't have run any connection code). `hdbcli` (dead
  SAP HANA driver) is intentionally LEFT for now — `drip_feed.py` / `sap_hana_extract.py` /
  `seed/sap_hana/load_berka.py` still import it; remove it as part of Task 1/3's rename+delete.
- **Databricks driver install caveat** (same failure class as `teradatasql` §11): `databricks-sdk`
  command-execution needs a context — `create_and_wait` a context first, then `execute_and_wait`
  with `context_id`, else you get `missing contextId`. Recorded so the next session doesn't
  re-derive it.

**Next session (unchanged target, refined pointers)**: execute `NEXT_BUILD_KICKOFF.md`'s 6 tasks.
Reminders: (1) resume the ClearScape Teradata environment before any live Teradata run; (2) the
Salesforce auth-flow doc-correction (Client Credentials Flow, not username-password — in `ADR-006`
Add #2, `.env.example`, `journey/07_PIPELINE_SPEC.md`) is still owed, do it with Task 3; (3)
Postgres + MS SQL Server (Docker) were never set up this session — stand them up before their
extractors can run live; (4) live creds now exist for a real Fasa A→D run — capture real evidence
into `journey/08_SERVING_AND_EVIDENCE.md`, don't settle for dev-loop-only.

**2026-07-14 — batch-1 setup detail (Salesforce/Teradata/OBP/Kaggle):** This session did the
prerequisite infra/credential setup the prior session's hand-off asked for, walked
interactively with the owner (trial signup, Connected App / External Client App config,
ClearScape provisioning, OBP sandbox registration, Kaggle key) — see the four live smoke-test
results below. `.env` is filled with real values (never pasted into chat; only lengths/
structure were inspected to debug auth failures). Postgres/MSSQL (Docker) were NOT touched
this session — still open.

- **Salesforce**: connected via **Client Credentials Flow** (Consumer Key + Secret + My Domain
  host only — NOT the username-password/ROPC flow `ADR-006` Addendum #2 and `.env.example`
  currently describe). Root cause chain worked through live, in order: (1) SOAP login (triggered
  by passing `security_token`) is disabled by default on this org → (2) REST OAuth "password"
  grant needs password+token concatenated, not passed separately, still got `invalid_grant` → (3)
  this org's **External Client App** model doesn't expose a Username-Password flow toggle at all
  (Salesforce has been deprecating ROPC for new apps) → (4) switched to **Client Credentials
  Flow**, enabled it in Settings → Flow Enablement, set **Run As** to the owner's own System
  Administrator user (`rdjluqman.av1.28b711d79d51@agentforce.com` — Salesforce auto-suffixed the
  username domain to `agentforce.com`, confirmed via Setup → Users this is still the real admin
  account, not a restricted service user) → connected successfully. Verified live: `sf.query()`
  ran, and both `Contact.birth_number__c` / `Contact.berka_client_id__c` custom fields (created
  manually via Object Manager, per `journey/05_STTM.md`'s Berka→Salesforce mapping) were
  confirmed present via `Contact.describe()`. **Doc-correction owed, not yet made**: `ADR-006`
  Add #2, `.env.example`'s Salesforce comment, and `journey/07_PIPELINE_SPEC.md`'s "OAuth
  username-password flow" line all need updating to say Client Credentials Flow — do this
  alongside Task 3 (`salesforce_extract.py`) in the next session, don't silently build against
  the stale doc language.
- **Teradata**: provisioned via **ClearScape Analytics Experience** (not Vantage Express — avoids
  the VM/network-exposure setup R-39 warns about; ClearScape gives a directly internet-reachable
  hosted instance). Connected live with `teradatasql.connect(host, user, password)` — only those
  three vars needed (confirmed by reading `pipeline/extract/teradata_extract.py`, no separate
  database/port var required). Note: the `teradatasql` pip install was initially broken/
  incomplete in this environment (installed package had only README/samples, no driver code,
  `teradatasql.connect` raised `AttributeError`) — fixed with `pip install --force-reinstall
  --no-cache-dir teradatasql`, now `teradatasql==20.0.0.63`. `requirements.txt`'s `>=17.20` pin
  is satisfied.
- **OBP (Open Bank Project sandbox)**: registered a sandbox user + a Consumer (Public app type,
  DirectLogin doesn't use a client secret) via the API Explorer's "Register a consumer" form.
  Connected live using the REAL `pipeline/extract/obp_client.py` code (not a reimplementation):
  `OBPClient()._get_direct_login_token()` succeeded, `_request("/obp/v4.0.0/my/accounts...")`
  returned 0 accounts (expected — brand-new sandbox user, no seeded data, not an error).
- **Kaggle**: API key obtained and verified — `KaggleApi().authenticate()` + `dataset_list(search=
  "home credit default risk")` returned 20 results live. Closes part of the original "Known
  blocker" (no Kaggle API credentials) named in `CLAUDE.md` and `BUILD_REPORT.md` §8.1 — the
  Kaggle CSVs themselves (Home Credit, PaySim) still haven't been downloaded into this repo, that
  remains next-session work.

**Next session**: execute `NEXT_BUILD_KICKOFF.md`'s 6 tasks for real (source-key rename,
`seed/salesforce/load_berka.py`, `pipeline/extract/salesforce_extract.py`, `silver_crm.py` +
`sil_crm_case`, orchestration config, health-mart source map), fix the Salesforce auth-flow doc
language named above, then run the 4 gates + `unittest discover tests`, and — since live,
verified credentials now exist for 4 of 5 sources — actually run Fasa A→D live (not just
code-written) and capture real evidence into `journey/08_SERVING_AND_EVIDENCE.md` per the
existing hand-off note below.

**2026-07-14 (earlier this same date) — source #4 swapped SAP HANA Cloud → Salesforce**
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
