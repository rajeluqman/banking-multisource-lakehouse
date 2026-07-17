# 08 — Serving & Evidence

> Status at Fasa 0 (bootstrap): this doc is the CONTRACT for what evidence Fasa D/E must capture.
> The actual query outputs are pasted in as each fasa completes — this is not yet possible before
> Gold exists. Updated incrementally; do not backfill fake output here.

## What is actually served, to whom, how
- **Dev loop**: local Spark/subset queries against local-disk Delta tables, run ad-hoc from
  `pipeline/gold/` scripts or a notebook — read/write mechanism, not a UI.
- **Canonical**: Databricks SQL / notebook queries against Unity-Catalog-governed Gold tables on
  the disposable trial, screenshotted (D-01 Add #3 evidence-first rule).
- **Serving veneer (Fasa E, optional)**: Snowflake external tables over the Gold S3 prefix + one
  Power BI page covering BQ-01 + BQ-02, or a DuckDB query if no live Snowflake account.

## Per-BQ evidence (2026-07-17, refreshed against REAL full-Kaggle-scale S3 Gold — 8/10 clean or fixed, 1 live defect + 1 reconciliation regression open)

**Why this section exists**: the 2026-07-15 evidence below (kept further down, historical) was
captured against a small D-14 dev-loop sample (e.g. `fact_txn` 20,750 rows total). Session 9
(2026-07-17, same day as this refresh) wired the Gold layer against the REAL full-Kaggle-scale S3
Silver data seeded earlier that session — PaySim alone is 6,362,620 real transaction rows, Home
Credit 307,511 real applications — and proved all 16 Gold tables have real Delta commits
(BUILD_REPORT.md §24). This section re-runs the BQ-01..10 queries directly against those real S3
Gold tables (read-only — no mart rebuild, no pipeline file touched) to replace the stale numbers.

**Method**: local Spark, `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` (ADR-009's recorded
environment fact), reading `s3://banking-lakehouse-pipeline/banking/gold/` directly via an ad hoc
`hadoop-aws:3.3.4` S3A config (the checked-in `pipeline/common/spark_session.py` doesn't need
this for the canonical Databricks run, only for this local read-only query — no pipeline file was
changed to do this). $0 cost — read-only queries against already-materialized Delta tables.

**Honest headline, not "10/10"**: 7 of 10 BQs were clean at real full scale on first pass. Two
(BQ-04, BQ-09) had real, previously-undetected-at-full-scale defects, surfaced rather than
silently fixed in the moment — evidence capture came first, fixes went through governance after
(`@staff-data-engineer` sign-off per CLAUDE.md's Gold-layer gate, since `pipeline/gold/` is a
governed path). **BQ-04 is now fixed and locally re-verified** (code corrected, `@staff-data-engineer`-
ruled design, real S3 Silver re-read confirms `application_count=307511`) but the canonical S3
Gold table won't reflect it until the next Databricks Gold job run. **BQ-09 remains open** — it's
a cost/scope decision (rebuild a 6.9M-identity crosswalk), not a pure code fix, and needs
`@finops` alongside `@staff-data-engineer`. BQ-10 also surfaces a new small reconciliation
regression (OBP), not yet investigated. Both are flagged as next-session candidates, not hidden.

| BQ | Mart | Query location | Output captured | Status |
|---|---|---|---|---|
| BQ-01 | mart_customer_360 | pipeline/gold/mart_customer_360.py | **real**, 329,984 rows — top by txn_count: `CUST_BK_2295 txn_count=6 total_txn_value=2069.84 product_count=1` (many ties at max txn_count=6 now that PaySim's per-customer txn_count is capped by BQ-09's xwalk-coverage gap below — only 32,976/6,362,620 PaySim rows resolve to a customer at all) | **PROVEN** |
| BQ-02 | mart_fraud_daily | pipeline/gold/mart_fraud_daily.py | **real**, 62 rows — top by value: `2026-07-02 TRANSFER fraud_txn_count=158 fraud_txn_value=319264672.56` | **PROVEN** |
| BQ-03 | mart_fraud_followup | pipeline/gold/mart_fraud_followup.py | **real** — `fraud_event_count=8213, within_sla_count=0, within_sla_pct=0.0` (same disclosed-not-hidden caveat as before: synthetic seed-time Salesforce Case data, not correlated with real fraud events — `seed/salesforce/load_berka.py`'s `_generate_cases`) | **PROVEN** |
| BQ-04 | mart_loan_funnel | pipeline/gold/mart_loan_funnel.py | **FIXED same session, code corrected + locally re-verified against real S3 Silver (read-only repro, not yet redeployed to the canonical S3 Gold table — see note below)**: root cause was `mart_loan_funnel.py`'s `application.join(previous, "SK_ID_CURR", "left")`, a 1:N join (`previous_application` has 1,670,214 rows across only 338,857 distinct `SK_ID_CURR`, ~4.9 rows per application) that fanned `application_count` out to 1,430,155 instead of the real 307,511 distinct applications — violated the mart's own "one row per app_month" grain (journey/04_DATA_MODEL.md:30). `@staff-data-engineer` ruled **Option A**: aggregate `application_count` from `application` alone (its native 1:1 grain), aggregate `approval_rate_pct`/`avg_days_to_decision` separately from the `previous_application` join, then join the two aggregates at `app_month` — not Option B (collapsing `previous_application` to one row per customer, which the ruling rejected as "destroying signal ... biases approval_rate/avg_days toward recency and survivorship"). Locally re-verified (read-only, no write): `app_month=2026-07, application_count=307511` (now exactly matches `application`'s real distinct-`SK_ID_CURR` count), `approval_rate_pct=62.68`, `avg_days_to_decision=880.37` — **named as an event-weighted PROXY**, not the current application's own outcome, since `application` itself carries no approval/decision field (`previous_application` is a different loan population, each customer's own history of OTHER prior loans). `DAYS_DECISION`/`NAME_CONTRACT_STATUS` column existence also schema-verified this session (journey/03_DATA_REQUIREMENTS.md, `(unverified)` tag cleared). | **CODE FIXED + LOCALLY PROVEN; canonical S3 Gold table still holds the stale 1,430,155-row output until the next Databricks Gold job run redeploys it (out of this session's scope — no Databricks job trigger from this environment)** |
| BQ-05 | mart_risk_segment | pipeline/gold/mart_risk_segment.py | **real**, 307,511 rows — exactly matches `application`'s row count (confirms the customer grain holds correctly here, no fan-out) — sample: `CUST_108201 income_band=HIGH NAME_INCOME_TYPE=State servant is_default=1` | **PROVEN** |
| BQ-06 | mart_cross_sell | pipeline/gold/mart_cross_sell.py | **real**, 45 qualifying customers — top by balance: `CUST_BK_1384 current_balance=21782.13 last_txn_ts=2026-05-15 prior_campaign_outcome=unknown subscribed_term_deposit=false` | **PROVEN** |
| BQ-07 | mart_dormancy | pipeline/gold/mart_dormancy.py | **real**, 310,119 rows — longest-dormant: `CUST_BK_1475 last_txn_ts=2023-04-14 days_since_last_txn=1190` | **PROVEN** |
| BQ-08 | mart_daily_flows | pipeline/gold/mart_daily_flows.py | **real**, 469 rows — `2026-07-15 total_in=391416934.33 total_out=1605248429.29 net_flow=-1213831494.96`; `total_deposits_snapshot=1282397.59` (unchanged from the 07-15 evidence — Berka's own account population/balances didn't grow at full scale, only PaySim's transaction volume did) | **PROVEN** |
| BQ-09 | fact_txn x dim_customer | pipeline/gold/fact_txn.py + pipeline/gold/dim_customer.py | **real data, LIVE DEFECT FOUND**: `fact_txn` now holds 6,363,370 real rows (6,362,620 paysim + 750 berka — the paysim count matches Silver's real full ingest 1:1, confirming the union itself is complete). But only 20,859/6,363,370 (0.33%) join to a non-NULL `customer_id`. Verified directly: `dim_customer_xwalk`'s paysim leg carries only 32,976 sampled native keys (`seed/build_xwalk.py`'s `--paysim-sample` flag, a deliberate D-14 dev-loop-scale cap — BUILD_REPORT.md Task 4), against the real Bronze/Silver PaySim population's 6,353,307 distinct `nameOrig` identities. **This is NOT a relapse of the session-3-fixed masking bug** — every one of the 32,976 sampled identities resolves correctly (0 unexpected NULLs within the sampled population, confirming the join mechanism itself is correct). It is a separate, previously-undetected coverage gap: the crosswalk was never rebuilt to match Bronze/Silver once those were re-seeded at real full Kaggle scale this session. | **NOT PROVEN AT FULL SCALE — real, live finding; needs `@staff-data-engineer` + `@finops` decision on xwalk rebuild cost/scope (next session)** |
| BQ-10 | mart_pipeline_health | pipeline/gold/mart_pipeline_health.py | **real**, latest run (2026-07-17 08:24:30 UTC) — `postgres` 307511/307511 reconciled=true, `mssql` 6362620/6362620 reconciled=true, `salesforce` 181/181 reconciled=true, `teradata` 45211/45211 reconciled=true, **`obp` 20 bronze/21 silver reconciled=false** (NEW finding — stable across all 3 runs captured this session; the 2026-07-15 evidence showed `obp` 20/20 reconciled=true) | **4/5 sources PROVEN reconciled; obp regressed to reconciled=false — needs investigation (next session)** |

**Not rebuilt/not re-verified this session** (out of scope per the task): OBP's Silver-terminal
status (ADR-005 Add #2, settled, not re-litigated) and the 4 un-Silver'd Home Credit tables
(`bureau_balance`/`POS_CASH_balance`/`credit_card_balance`/`installments_payments`, locked scope,
no BQ needs them) were untouched.

---

## Per-BQ evidence — HISTORICAL, SUPERSEDED BY THE 2026-07-17 REFRESH ABOVE (2026-07-15, third session same day — ALL 5 sources live, small dev-loop-scale sample, 10/10 BQs PROVEN at that scale)

**Real infra used this run**: same Kaggle/Docker/local-Spark base as the second session, PLUS all
3 previously-blocked sources brought fully live in one continuous session: **Salesforce**
(org setup completed by the owner mid-session, re-verified via `describe()` — 100% clean),
**Teradata** (ClearScape resumed by the owner — confirmed live via `teradatasql.connect()`, not
assumed), **OBP** (rewritten to pull real public-sandbox demo data instead of the always-empty
`/my/accounts`). All 5 sources now show `reconciled=true` in `mart_pipeline_health` — **no Slack
alert fired**, for the first time in this project's history.

This was not a clean run — 12 real, previously-undetected bugs were found and fixed by actually
executing code that had never run against live infra before (full list below). Two were serious
enough to silently corrupt every customer-level number in Gold if left unfixed: the entire
PaySim leg of `fact_txn`/`fact_card_fraud` resolved to a **NULL `customer_id` for 100% of rows**
(masked Silver column joined against an unmasked xwalk key — never caught before because
`fact_txn.py` had never successfully run until this session), and `seed/build_xwalk.py` sampled
PaySim customer IDs **independently** from what `seed/mssql/load_paysim.py` actually loaded
(different RNG namespace — two unrelated 20k draws from a 6.36M-row pool, ~62 rows overlapping
by pure chance). Both are fixed and reverified with 0 NULL `customer_id` across 20,750 `fact_txn`
rows and a 100% `dim_customer_xwalk`↔MSSQL overlap.

| BQ | Mart | Query location | Output captured | Status |
|---|---|---|---|---|
| BQ-01 | mart_customer_360 | pipeline/gold/mart_customer_360.py | **real**, 329,984 rows — top by txn_count: `CUST_BK_1179 txn_count=6 total_txn_value=413972.66 product_count=1` (2026-07-15 R-14/D-12 fix: was `431259.62` — a real, live currency-mixing bug, this customer's 5 Berka/CZK legs were summed with 1 PaySim/MYR leg with zero conversion; BUILD_REPORT.md §16) | **PROVEN** |
| BQ-02 | mart_fraud_daily | pipeline/gold/mart_fraud_daily.py | **real**, 28 rows — `2026-06-22 CASH_OUT fraud_txn_count=1 fraud_txn_value=48299.77` | **PROVEN** |
| BQ-03 | mart_fraud_followup | pipeline/gold/mart_fraud_followup.py | **real** — `fraud_event_count=32, within_sla_count=0, within_sla_pct=0.0` (synthetic seed-time Case data, not correlated with real fraud events — a documented simulated signal, not a bug, see `seed/salesforce/load_berka.py`'s `_generate_cases`) | **PROVEN** |
| BQ-04 | mart_loan_funnel | pipeline/gold/mart_loan_funnel.py | **real** — `app_month=2026-07, application_count=5000, approval_rate_pct=0.92, avg_days_to_decision=922.76` | **PROVEN** |
| BQ-05 | mart_risk_segment | pipeline/gold/mart_risk_segment.py | **real**, 5000 rows — `CUST_310094 income_band=MEDIUM NAME_INCOME_TYPE=Working is_default=1` | **PROVEN** |
| BQ-06 | mart_cross_sell | pipeline/gold/mart_cross_sell.py | **real**, 87 qualifying customers — `CUST_397288 current_balance=12169.19 last_txn_ts=2025-10-13` (2026-07-15 R-14/D-12 fix: was `59361.9` — Berka's raw CZK balance was surfaced in Gold without conversion to the MYR reporting standard; BUILD_REPORT.md §16) | **PROVEN** |
| BQ-07 | mart_dormancy | pipeline/gold/mart_dormancy.py | **real**, 310,159 rows — longest-dormant: `CUST_BK_1475 last_txn_ts=2023-04-14 days_since_last_txn=1188` | **PROVEN** |
| BQ-08 | mart_daily_flows | pipeline/gold/mart_daily_flows.py | **real**, 469 rows — `2026-07-13 total_in=797035.1 total_out=3842271.6 net_flow=-3045236.5` (unchanged — this date had no Berka legs); `total_deposits_snapshot=1282397.59` (2026-07-15 R-14/D-12 fix: was `6255598.0` — Berka's raw CZK account-balance sum was reported as if it were already MYR, a 79% overstatement; BUILD_REPORT.md §16) | **PROVEN** |
| BQ-09 | fact_txn x dim_customer | pipeline/gold/fact_txn.py + pipeline/gold/dim_customer.py | **real** — 20,750/20,750 `fact_txn` rows join cleanly to `dim_customer` (0 NULL `customer_id`, both PaySim and Berka legs) — `CUST_PS_C1463592651 amount=280555.57 source=paysim` | **PROVEN** (was 0/20,750 before this session's fixes — see bug list) |
| BQ-10 | mart_pipeline_health | pipeline/gold/mart_pipeline_health.py | **real** — all 5 sources `reconciled=true`: `postgres` 5000/5000, `mssql` 20000/20000, `salesforce` 181/181, `teradata` 45211/45211, `obp` 20/20. No Slack alert fired. | **PROVEN** |

**All 10/10 business questions are now PROVEN against real, live infrastructure** — the first time
this has been true for this project.

### Bugs found and fixed this run (2026-07-15, third session)
1. **Salesforce seed data storage-limit blowout**: the Developer Edition org has a 5MB
   `DataStorageMB` cap; the prior session's `--sample 5000` load blew through it silently
   (Bulk API 2.0 returns HTTP success even when every record fails server-side — the seed
   script trusted `len(records)` as "loaded," not the job's real `numberRecordsFailed`).
   Live diagnosis: Transaction__c 100% failed, AccountContactRelation 100% failed, Case 100%
   failed, Account 47% failed. Fixed `seed/salesforce/load_berka.py`'s `_report()` to read
   real Bulk API job results, and rearchitected sampling to be ACCOUNT-rooted/coordinated
   (150 accounts → their real linked clients/transactions) instead of independently sampling
   every table — the only way to get meaningful relational density within the 5MB budget.
2. **AccountContactRelation 100% insert failure**: `INVALID_CROSS_REFERENCE_KEY: You can't
   associate a private contact with an account` — live-diagnosed as a genuine Salesforce
   platform rule (a Contact needs a primary `AccountId` before any additional/indirect
   `AccountContactRelation` can be created for it), not an org Setup toggle. Fixed by setting
   each client's primary account directly on `Contact.AccountId`, only using
   `AccountContactRelation` for a client's second+ account.
3. **The primary-relationship patch above didn't populate its own custom fields**: setting
   `Contact.AccountId` auto-creates a "direct" `AccountContactRelation` record in Salesforce,
   but that update has no way to set the auto-created record's `berka_disp_id__c`/
   `berka_disp_type__c`. Left unfixed, `sil_disp.type` was blank for all 181 rows, which
   silently broke `fact_txn.py`'s Berka leg (filters `type == "OWNER"` → matched nothing →
   100% NULL `customer_id`). Fixed by querying the auto-created records back and patching them.
4. **`pipeline/extract/salesforce_extract.py`'s AccountContactRelation SOQL was missing
   `berka_disp_type__c`** from its field list entirely — `silver_crm.py` expected it and
   crashed with `UNRESOLVED_COLUMN`.
5. **`pipeline/silver/silver_crm.py`'s `sil_account` MERGE crashed** (`DELTA_MULTIPLE_
   SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`) — the Account SOQL pulled ALL Accounts including
   8 pre-existing Developer Edition sample/demo records (Edge Communications, etc.) with a
   NULL `berka_account_id__c`, and Delta's MERGE treats multiple NULL-keyed source rows as an
   ambiguous match. Fixed by adding a `WHERE berka_account_id__c != null` filter to the extract.
6. **`fact_txn.py`'s Berka leg had an unqualified `col("type")` reference** — `AMBIGUOUS_
   REFERENCE`, since both `trans` and `disp` (post-join) carry a `type` column. Fixed with
   explicit dataframe aliasing.
7. **`fact_txn.py`'s `unionByName` crashed on an `is_fraud` type mismatch** — PaySim's leg is
   `long` (0/1), the Berka leg was a bare `lit(False)` (boolean). Fixed by casting to `long`.
8. **`fact_txn.py`'s `trans`-to-`disp` join fanned out transaction rows** for jointly-held
   accounts (an account with both an OWNER and a DISPONENT disp row) — every such account's
   transactions got duplicated once per disponent, silently double-counting `amount` in every
   downstream mart (750 real rows became 905 after the join). Fixed by restricting the join to
   `type == "OWNER"` (Berka: exactly one owner per account), restoring the one-row-per-
   transaction grain.
9. **The BIGGEST bug — both `fact_txn.py` and `fact_card_fraud.py`'s PaySim legs joined a
   MASKED Silver column against an UNMASKED xwalk key**: `card_txn.name_orig_masked` is
   last-4-masked at Silver (D-07), but `dim_customer_xwalk.native_key` was built from
   Bronze's unmasked `nameOrig` — these can never match. Live-caught: 100% NULL `customer_id`
   for all 20,000 PaySim `fact_txn` rows and all 32 `fact_card_fraud` rows. This had never
   been caught because neither Gold builder had successfully run before this session. Fixed
   by resolving identity via Bronze's raw `nameOrig` (never persisted into Gold, only used
   transiently for the join), then joining that resolution back to Silver by `txn_id` — the
   same "resolve identity, mask everything else" discipline the Berka leg already follows via
   `client_id` (R-38/D-07).
10. **`seed/build_xwalk.py` sampled PaySim customer IDs independently from `seed/mssql/
    load_paysim.py`'s actual seeded rows** — different `seeded_random` namespace
    (`"build_xwalk.paysim_sample"` vs `"paysim"`), so the xwalk's 20,000 PaySim keys and
    MSSQL's actual 20,000 seeded rows were two unrelated random draws from PaySim's
    6.36M-row pool. Only ~62 rows overlapped by chance (matches the statistically-expected
    ~63 for two independent 20k samples of 6.36M) — this is what made bug #9 above so hard
    to fully diagnose without checking the raw overlap directly. Fixed by making
    `build_xwalk.py` replicate `load_paysim.py`'s exact row sample (same RNG namespace,
    same pandas `.sample()` call) before extracting unique customer-shaped names — 100%
    overlap confirmed after the fix. Required re-seeding Teradata's `bank_marketing` too,
    since its `customer_id` assignment sampled from the (now-corrected) xwalk population.
11. **`seed/teradata/load_bank_marketing.py`'s Teradata DDL was written against SAP HANA
    syntax and never actually run before this session** (Teradata was the last source ever
    brought live) — `CREATE COLUMN TABLE` isn't valid Teradata DDL (needed plain/MULTISET
    `CREATE TABLE`), `PRIMARY KEY` columns need an explicit `NOT NULL`, `GENERATED ALWAYS AS
    IDENTITY` needs an explicit `(START WITH ... INCREMENT BY ...)` clause, trigger bodies
    need `BEGIN ATOMIC` not bare `BEGIN`, and `REFERENCING`-alias column references are bare
    `new_row.col`, not `:new_row.col`. All 6 fixed and live-verified (insert/update/delete
    triggers all confirmed to actually fire and log to `_cdc_log`).
12. **`pipeline/silver/silver_marketing.py` and `mart_pipeline_health.py` both only ever
    read Teradata's `bank_marketing_cdc` Bronze table** — legitimately near-empty, since the
    bulk seed lands via a separate one-time snapshot (`cdc_initial_snapshot.py`, R-40) that
    predates the CDC triggers. Live-caught: `silver_marketing` returned 0 rows against a
    freshly-seeded 45,211-row table. Fixed both to UNION the initial-snapshot Bronze table
    with any actual CDC overlay (update -> latest values win, delete -> excluded).
13. **`pipeline/extract/obp_client.py` called `/my/accounts`**, which is scoped to the
    authenticated sandbox user (always empty for a fresh user) — assumed the sandbox came
    pre-populated, never checked live. Live-corrected: the public OBP sandbox has ~199 real
    demo banks with public-view accounts/transactions (`/banks/{id}/accounts/public`); the
    "public" view ID also isn't a literal string, it's per-account (read from
    `views_available` where `is_public` is true). Rewritten to walk public banks → public
    accounts → each account's own public view's transactions — 20 real accounts, 183 real
    transactions landed, zero invented/seeded data. `silver_core_banking.py` also assumed
    `account_id`/`transaction_id` were literal Bronze column names; OBP's own PK field is
    `id` (transactions carry the owning account nested at `this_account.id`) — fixed.
14. **`mart_pipeline_health.py`'s `BRONZE_TO_SILVER_TABLE` had no entry for `bank_marketing`
    or `accounts`** (the Teradata/OBP Silver tables are actually named `campaign_response`/
    `obp_accounts`) — same class of defect as the previously-fixed R-30 bug, just not yet
    extended to the two sources that had never had real data before. Fixed.

"Proven" here means: real Docker/Kaggle/Salesforce/Teradata/OBP/local-Spark infrastructure, real
data, actual command output pasted above — not a claim to re-verify later.

### Bugs found and fixed in the second session (2026-07-15, Postgres/MSSQL/OBP only, historical)
Superseded by the third session above for Salesforce/Teradata/OBP-specific findings; kept here
as the historical record of what that session actually found and fixed:
1. `scripts/fetch_datasets.py`'s Berka Kaggle slug was a never-verified guess that doesn't
   exist; corrected to `marceloventura/the-berka-dataset` (verified via `dataset_list_files`
   returning account/card/client/disp/district/loan/order/trans — matches Berka's known tables).
   Home Credit's slug was a Kaggle *competition* (needs rules accepted on kaggle.com, no API
   path — 401'd live); switched to a verified dataset mirror instead.
2. `seed/build_xwalk.py`'s `_read_column` didn't pass `delimiter=";"` for Berka's `.asc` files
   (only `load_berka.py` did) — fixed. Also fixed an O(unique_customers × rows) rescan in the
   summary print (fine at the 41-row fixture-test scale, would have taken hours at real
   332,880-row scale — caught mid-run, killed the process, fixed to a single pass).
3. `pipeline/common/spark_session.py`'s local-mode Spark session had no Delta/Postgres/MSSQL
   JDBC jars configured at all — local Spark literally could not run before this fix
   (`configure_spark_with_delta_pip` + Maven coordinates added; `pyspark`/`delta-spark` added to
   `requirements.txt`, another real reproducibility gap).
4. `pipeline/extract/obp_client.py` called a `/my/transactions` endpoint that doesn't exist in
   the real OBP v4.0.0 API (confirmed live: 404) — fixed to the real per-account endpoint shape.
5. `pipeline/promote/promotion_gate.py`'s "batch" mode assumed Parquet universally, but OBP
   lands verbatim JSON (R-19) — fixed to detect payload shape. Also added a guard for
   zero-column payloads (an empty API response has no inferable schema; Delta's
   `DELTA_EMPTY_DATA` would otherwise reject it).
6. **All 14 `pipeline/gold/*.py` builder modules were missing the `main() -> int` entrypoint**
   `pipeline/orchestrate.py` requires — every one would have crashed with `AttributeError` the
   first time the orchestrator ever tried to run a Gold stage (never exercised before this
   session). Added `main()` to all 14, mirroring the Silver modules' existing convention.
7. `pipeline/silver/silver_fraud.py` never renamed PaySim's `type` column to `txn_type`, despite
   the STTM (`journey/05_STTM.md` line 79) explicitly specifying that mapping — `fact_card_fraud.py`
   correctly expected `txn_type` and failed with `UNRESOLVED_COLUMN` until this was fixed.
8. **Confirmed and fixed the pre-existing, previously-flagged R-30 defect** (`BUILD_REPORT.md`
   §13 item 2): `mart_pipeline_health.py`'s Silver row-count read included a `source` path
   segment that `merge_upsert` never writes. Fixed; `postgres`/`mssql` now correctly show
   `reconciled=true` with matching counts instead of `silver_row_count=NULL` for every source.
9. `mart_pipeline_health.py`'s `spark.createDataFrame(rows)` relied on type inference, which
   fails (`CANNOT_DETERMINE_TYPE`) whenever a column is `None` for every row — exactly the case
   before the orchestrator has ever run (`orchestrator_status`/`orchestrator_error`). Fixed with
   an explicit `StructType` schema.

"Proven" here means: real Docker/Kaggle/local-Spark infrastructure, real (sampled, per D-14)
data, actual command output pasted above — not a claim to re-verify later. (Historical note: at
the end of this second session, Salesforce/Teradata were still blocked on owner-only actions —
both were cleared and brought fully live in the third session above, which also found and fixed
14 further real bugs, several of them serious.)

## Proven vs claimed (the Volve lesson)
| Claim (in README/resume/docs) | Evidence (file:line or command output) | Status |
|---|---|---|
| "Four bootstrap gates green" (Fasa 0) | pasted gate output in `BUILD_REPORT.md` | pending — filled at Fasa 0 gate checkpoint |
| "10/10 BQs answerable" | this table, filled per-BQ as Fasa D ships | **8/10 clean or fixed at real full scale (2026-07-17) — BQ-04 fixed + locally proven, pending a Databricks redeploy to the canonical S3 table; BQ-09 has an open xwalk-coverage gap (cost/scope decision); BQ-10 has an obp reconciliation regression — see "Per-BQ evidence (2026-07-17...)" above** |
| "Landing→Bronze isolation proven" | Fasa B gate proof (kill-and-rerun, partial-arrival quarantine) | pending Fasa B |
| "PII masked before Gold" | grep/query proof, no unmasked account/card/birth_number in Gold | pending Fasa C/D |

No claim is written into a README/resume bullet for this project until its row here is `proven`,
not `unverified` — per the anti-shortcut reconcile-before-done rule.

## Resume-claim reconciliation
Deferred to `BUILD_REPORT.md` (final self-audit) once Fasa D/E are complete — mirrors the
pipeline_retrofit `INTERVIEW_GUIDE.md` pattern (resume bullet → repo evidence → flagged if
unsupported). Not duplicated here to avoid two documents drifting out of sync; this doc holds the
per-BQ query evidence, `BUILD_REPORT.md` holds the full resume-claim reconciliation.
