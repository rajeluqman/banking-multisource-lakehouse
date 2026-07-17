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

## Per-BQ evidence (2026-07-17, refreshed against REAL full-Kaggle-scale S3 Gold — 10/10 PROVEN)

**Why this section exists**: the 2026-07-15 evidence below (kept further down, historical) was
captured against a small D-14 dev-loop sample (e.g. `fact_txn` 20,750 rows total). Session 9
(2026-07-17, same day as this refresh) wired the Gold layer against the REAL full-Kaggle-scale S3
Silver data seeded earlier that session — PaySim alone is 6,362,620 real transaction rows, Home
Credit 307,511 real applications. This section re-runs the BQ-01..10 queries directly against the
real S3 Gold tables to replace the stale numbers, and documents 3 real defects found, fixed,
redeployed, and independently artifact-verified in the same session (not left for "next session").

**Method**: local Spark, `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` (ADR-009's recorded
environment fact), reading `s3://banking-lakehouse-pipeline/banking/gold/` directly via an ad hoc
`hadoop-aws:3.3.4` S3A config for read-only queries ($0). All fixes went through the governed
process (`@staff-data-engineer` sign-off before touching `pipeline/gold/`/`pipeline/silver/`,
`@finops` sign-off before paid Databricks runs), and every redeploy was independently verified at
the artifact level (boto3 `_delta_log` / direct table reads) per ADR-009's "never trust job
SUCCESS alone" doctrine — including catching and fixing a real operational mistake made mid-session
(below).

**The honest full arc, not just the clean ending**: the first evidence pass found 7/10 BQs clean
and 3 real, previously-undetected-at-full-scale defects (BQ-04, BQ-09, BQ-10) — surfaced, not
silently patched. Each fix was gated through the appropriate specialist before any code changed:
- **BQ-04** (`mart_loan_funnel.py` grain fan-out): `@staff-data-engineer` ruled the fix design
  (aggregate at native grain, don't fan out via the join). Fixed, PR #10, redeployed, artifact-verified.
- **BQ-10** (`mask_last4()`+`merge_upsert()` NULL-merge-key defect in `sil_obp_accounts`):
  `@staff-data-engineer` found a SECOND, more severe defect during review — the accounts↔transactions
  FK join was ALSO broken (masked PK vs unmasked FK, 0/183 matching). Ruled: never mask an
  identity/join key; split into a raw key (for MERGE/FK) + a derived `_last4` column (for exposure).
  journey/09_SECURITY_AND_ACCESS.md's D-07 doctrine clarified. Fixed, PR #11, poisoned table
  deleted+recreated, redeployed, artifact-verified (FK join now 183/183, `obp` reconciled=true).
- **BQ-09** (`dim_customer_xwalk` PaySim leg capped at a 32,976-row dev-loop sample vs. the real
  6.9M-identity population): `@staff-data-engineer` ruled this was finishing an already-locked D-14
  requirement, not new scope. `@senior-data-engineer` fixed a real OOM in the rebuild script
  (memory-safe streaming patch, verified). The full-scale artifact (278MB) couldn't be git-committed
  (no Git LFS, GitHub's 100MB hard limit) — `@staff-data-engineer` ruled a load-path change (S3 Delta
  artifact instead of a git CSV), recorded as **ADR-005 Addendum #3**. `@finops` approved the
  12-task downstream Gold redeploy. PR #12, merged.
- **A real incident, caught and fixed the same session**: the BQ-09 downstream redeploy
  (`dim_customer_xwalk` + 11 dependents) reported all 12 tasks `SUCCESS`, but artifact verification
  found `fact_txn`/`fact_card_fraud`/`fact_loan_application` **exactly doubled** (12,726,740 =
  2×6,363,370, etc.) — these 3 tables use `mode("append")` by design, and the redeploy re-ran them
  without deleting existing data first (an operational gap, not a new code bug). Per ADR-009,
  escalated to `@staff-data-engineer` as Incident Commander before any further paid run — ruled the
  fix mechanism correct but caught a gap in the plan (verify `dim_customer_xwalk`/`dim_customer`
  weren't ALSO silently doubled, don't assume from the dependency graph) and added
  `mart_pipeline_health` as a "witness" task. Both free checks passed (append-mode class confirmed
  exactly 3 members; the 2 overwrite-mode tables confirmed NOT doubled). One remediation run (11
  tasks) fixed it — verified `fact_txn` back to exactly 6,363,370 with **100.00% customer_id fill
  rate** (was 0.33% before BQ-09's fix even started).

| BQ | Mart | Query location | Output captured | Status |
|---|---|---|---|---|
| BQ-01 | mart_customer_360 | pipeline/gold/mart_customer_360.py | **real**, 4,462,220 rows (now matches the true unique bank-wide customer count post-xwalk-fix) — top by txn_count: `CUST_BK_3441 txn_count=12 total_txn_value=1330100.80 product_count=1` | **PROVEN** |
| BQ-02 | mart_fraud_daily | pipeline/gold/mart_fraud_daily.py | **real**, 62 rows — top by value: `2026-07-02 TRANSFER fraud_txn_count=158 fraud_txn_value=319264672.56` (unaffected by the xwalk fix — aggregates over all transactions regardless of customer_id resolution) | **PROVEN** |
| BQ-03 | mart_fraud_followup | pipeline/gold/mart_fraud_followup.py | **real** — `fraud_event_count=8213, within_sla_count=0, within_sla_pct=0.0` (same disclosed-not-hidden caveat as before: synthetic seed-time Salesforce Case data, not correlated with real fraud events — `seed/salesforce/load_berka.py`'s `_generate_cases`) | **PROVEN** |
| BQ-04 | mart_loan_funnel | pipeline/gold/mart_loan_funnel.py | **FIXED, redeployed, artifact-verified**: root cause was a 1:N join (`application`→`previous_application`) fanning `application_count` out to 1,430,155 instead of the real 307,511 — violated the mart's own "one row per app_month" grain. `@staff-data-engineer`-ruled fix (aggregate at native grain, join at reporting grain). PR #10 merged; Databricks job `456069514400579` run `1024722577817236` (scoped, `mart_loan_funnel` only), `result_state=SUCCESS`; boto3 `_delta_log` + direct read confirm `application_count=307511, approval_rate_pct=62.68, avg_days_to_decision=880.37` (named an event-weighted proxy, not the current application's own outcome, since `application` carries no approval field itself). | **PROVEN** |
| BQ-05 | mart_risk_segment | pipeline/gold/mart_risk_segment.py | **real**, 307,511 rows — exactly matches `application`'s row count (confirms the customer grain holds correctly, no fan-out) — sample: `CUST_108201 income_band=HIGH NAME_INCOME_TYPE=State servant is_default=1` | **PROVEN** |
| BQ-06 | mart_cross_sell | pipeline/gold/mart_cross_sell.py | **real**, 6 qualifying customers (down from an earlier 45 pre-xwalk-fix reading — EXPECTED, not a regression: the "no card" filter reads `fact_txn`'s PaySim leg, which now correctly resolves ~100% of customers instead of 0.33%, so far more customers correctly show an existing card and are correctly excluded from the cross-sell list) — top by balance: `CUST_BK_822 current_balance=15256.92 last_txn_ts=2026-01-12` | **PROVEN** |
| BQ-07 | mart_dormancy | pipeline/gold/mart_dormancy.py | **real**, 285,264 rows (down from an earlier 310,119 pre-xwalk-fix reading — EXPECTED: more customers now show real recent PaySim activity via the fixed identity resolution, so fewer are dormant) | **PROVEN** |
| BQ-08 | mart_daily_flows | pipeline/gold/mart_daily_flows.py | **real**, 469 rows — `2026-07-16 total_out=449703481.30`; `total_deposits_snapshot=1282397.59` (unaffected by the xwalk fix — Berka-only, no PaySim identity dependency) | **PROVEN** |
| BQ-09 | fact_txn x dim_customer | pipeline/gold/fact_txn.py + pipeline/gold/dim_customer.py | **FIXED, redeployed, artifact-verified**: root cause was `dim_customer_xwalk`'s PaySim leg capped at a 32,976-row D-14 dev-loop sample against the real 6.9M-identity population (only 0.33% of `fact_txn` PaySim rows resolved a `customer_id`). `@staff-data-engineer`-ruled fix: rebuild the xwalk at full scale (memory-safe streaming patch by `@senior-data-engineer`, OOM'd otherwise — verified 1GB peak RSS, ~76s, byte-identical to the old algorithm) and move the canonical artifact from a git-committed CSV to an S3 Delta artifact (ADR-005 Addendum #3 — the full-scale artifact is ~278MB, no Git LFS configured, GitHub hard-rejects >100MB). PR #12 merged. `@finops`-approved 12-task downstream redeploy surfaced a real operational incident (3 append-mode fact tables doubled — see arc above), root-caused and fixed via `@staff-data-engineer` as Incident Commander (ADR-009) with one remediation run. Final artifact-verified state: `dim_customer_xwalk`=7,236,379 rows, `fact_txn`=6,363,370 rows with **0 NULL `customer_id`, 100.00% fill rate** (was 0.33%). | **PROVEN** |
| BQ-10 | mart_pipeline_health | pipeline/gold/mart_pipeline_health.py | **FIXED, redeployed, artifact-verified**: root cause was `mask_last4()` masking `sil_obp_accounts.account_id` — which doubled as the table's own MERGE key AND `sil_obp_transactions.account_id`'s FK target. Two live bugs: the FK join could never match (masked PK vs unmasked FK — was 0/183, now 183/183), and mask-to-NULL-under-4-chars broke MERGE's own `NULL != NULL` idempotency, duplicating a row every Silver rebuild. `@staff-data-engineer` ruling: never mask an identity/join key — raw key stays for MERGE/FK, a derived `account_id_last4` column carries the masked value for exposure (D-07 clarified in journey/09_SECURITY_AND_ACCESS.md). PR #11 merged; poisoned table deleted+recreated (staff-DE's explicit remediation — MERGE can't self-heal an already-persisted duplicate); redeployed; boto3/direct-read confirms `sil_obp_accounts` 20/20 distinct raw `account_id` (0 NULLs), and `mart_pipeline_health`'s latest run shows **all 5 sources reconciled=true** (postgres, mssql, salesforce, teradata, obp). | **PROVEN** |

**Not touched this session** (out of scope, unrelated to BQ-01..10): OBP's Silver-terminal status
(ADR-005 Add #2, settled, not re-litigated) and the 4 un-Silver'd Home Credit tables
(`bureau_balance`/`POS_CASH_balance`/`credit_card_balance`/`installments_payments`, locked scope).

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
| "10/10 BQs answerable" | this table, filled per-BQ as Fasa D ships | **10/10 PROVEN at real full scale (2026-07-17) — 3 real defects (BQ-04/09/10) found, fixed, redeployed, and independently artifact-verified in the same session, including a mid-session operational incident (3 doubled fact tables) caught and remediated — see "Per-BQ evidence (2026-07-17...)" above** |
| "Landing→Bronze isolation proven" | Fasa B gate proof (kill-and-rerun, partial-arrival quarantine) | pending Fasa B |
| "PII masked before Gold" | grep/query proof, no unmasked account/card/birth_number in Gold | pending Fasa C/D |

No claim is written into a README/resume bullet for this project until its row here is `proven`,
not `unverified` — per the anti-shortcut reconcile-before-done rule.

## Resume-claim reconciliation
Deferred to `BUILD_REPORT.md` (final self-audit) once Fasa D/E are complete — mirrors the
pipeline_retrofit `INTERVIEW_GUIDE.md` pattern (resume bullet → repo evidence → flagged if
unsupported). Not duplicated here to avoid two documents drifting out of sync; this doc holds the
per-BQ query evidence, `BUILD_REPORT.md` holds the full resume-claim reconciliation.
