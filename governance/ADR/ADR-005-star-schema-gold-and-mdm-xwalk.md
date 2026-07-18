# ADR-005 — Gold is a star schema; `dim_customer_xwalk` is the conformed-dimension keystone

**Status:** Accepted
**Date:** 2026-07-05
**Owners:** owner (ratified), architect (sign-off)
**Context refs:** `01_OPUS_DECISIONS.md` D-04, D-07 in the planning lab;
`04_BUSINESS_QUESTIONS.md` (8 of 10 BQs require the crosswalk); R-23, R-24, R-29.

## Context
Unlike creative_intelligence_lab (which chose a hybrid asset-graph + star model, CIL ADR-002),
this project's Gold layer has no graph-shaped entity — its hard problem is Master Data Management
across four sources with **no shared key**, not near-duplicate content relationships. A star
schema with one conformed dimension (the crosswalk) is the correct fit and is simpler to defend.

## Decision
- **Modelling approach: star schema.** `dim_customer` (golden record, survivorship-resolved),
  `dim_date`, plus source-scoped facts (`fact_txn`, `fact_card_fraud`, `fact_loan_application`) and
  the 7 business marts named in `journey/02_BUSINESS_QUESTIONS.md`.
- **`dim_customer_xwalk` is generated at seed time (D-04)**, not derived at Gold build time — it
  maps one bank-wide `customer_id` to each SEEDABLE source's native key (`SK_ID_CURR`, `nameOrig`/
  `nameDest`, `client_id`+`account_id`). This is the conformed dimension every fact joins through;
  8 of the 10 locked business questions are impossible without it (proof that the MDM layer needs
  to exist, not decoration). **OBP `account_id` is NOT a member key** — OBP is deliberately
  Silver-terminal (Addendum #2): its public-sandbox accounts have no conformable customer identity
  and it is a live (non-seed-deterministic) source.
- **Golden-record survivorship**: source priority **CRM (Berka) > loans (Home Credit) > cards
  (PaySim)**, with Teradata (Bank Marketing) enriching on the same `customer_id` (R-38); latest
  `updated_at` as the tiebreak within a source (R-23/R-24). **OBP is not a survivorship tier**
  (Addendum #2 — the earlier "core (OBP)" tier is withdrawn; OBP never enters the golden record).
- **Late-arriving dimension handling (R-29)**: unknown-member key (`-1`) for a fact row whose
  customer hasn't yet appeared in the CRM extract, re-linked on the next run.
- **Clean-model doctrine (non-negotiable, inherited from the kit template)**: 1 table = 1 grain =
  1 business entity, bridge tables (not CTEs) for N:N, serving = view never a duplicated physical
  copy, one explicit SCD strategy per table (Type 1 for `dim_customer` attributes changed by
  survivorship re-run; Type 2 NOT used in v1 — history-of-change tracking is out of scope, named
  here rather than silently absent).

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| Graph/asset model (CIL's ADR-002 pattern) | CIL's hard problem was near-duplicate CONTENT relationships (video clips reusable together); this project's hard problem is IDENTITY across disjoint sources — a star schema with a conformed MDM dimension is the correct, simpler fit, not graph-shaped |
| Resolve crosswalk at Gold build time instead of seed time | Rejected — the crosswalk is the seed-time contract that makes the whole multi-source simulation coherent (D-04); deriving it lazily at Gold time would make Silver-layer joins undefined and duplicate the resolution logic in multiple places |
| Type 2 SCD on `dim_customer` | Deferred, not rejected outright — named as explicitly OUT of v1 per the clean-model doctrine's "what's deliberately out stays named" rule; would require a real business need (regulatory point-in-time reporting) not present in the locked BQ list |

## Consequences
- Every Gold-layer customer-scoped table has exactly one join path to identity: through
  `dim_customer_xwalk` → `dim_customer`. A model that invents a second customer-resolution path is
  a Clean-ERD Doctrine violation (architect veto).
- Right-to-erasure (D-16 §7, R-34) becomes tractable specifically because of this single
  conformed dimension — a compliance capability that falls out of the MDM design, not a separate
  build.
- Does NOT decide table-by-table grain — that's `journey/04_DATA_MODEL.md`'s grain-declarations
  table, filled per table as Fasa D builds each one.

## Addendum log
**Addendum #1 (2026-07-15, @staff-data-engineer sign-off) — `dim_fx_rate`, a new conformed
dimension implementing locked D-12/R-14 (currency normalization).** Not new scope — D-12
("Gold normalizes to one reporting currency (MYR) via a static FX seed table",
journey/05_STTM.md) and R-14 (journey/06_DQ_PLAN.md) were locked decisions that had never
actually been built; `mart_daily_flows.py`/`mart_customer_360.py` were silently summing
CZK+MYR together (real, live bug — BUILD_REPORT.md §16). No ADR-000 intake needed.
- **Grain**: one row per `currency_code` (`seed/artifacts/fx_rates.csv` →
  `pipeline/gold/dim_fx_rate.py`, same seed-artifact-load pattern as `dim_customer_xwalk.py`).
  Static (`rate_as_of` is metadata only, not part of the PK/join) — a date-versioned rate
  would force an as-of temporal join and change fact grain semantics; D-12 explicitly wants
  a static seed table, live BNM OpenAPI enrichment stays optional/out of scope.
- **Single resolution path** (this doctrine's own rule, line ~43): FX conversion happens
  ONCE, via `to_myr` (`pipeline/gold/common.py`), at the fact grain (`fact_txn.amount_myr`,
  `fact_card_fraud.amount_myr`) and once in the shared `latest_balance_per_account` helper
  (`current_balance_myr`) — no per-mart FX join. Native `amount`/`currency`/
  `current_balance` columns are kept, never overwritten (lineage/auditability).
- **Grain of `fact_txn`/`fact_card_fraud` unchanged** — adding `amount_myr` is an additive
  derived column, not a new grain or entity, so this does not require its own ADR beyond
  this addendum.
- **SCD**: static overwrite, same as `dim_customer_xwalk.py` (Type 1-equivalent, stated).

**Addendum #2 (2026-07-16, @staff-data-engineer sign-off) — OBP is deliberately Silver-TERMINAL,
NOT a conformed-dimension member. Ratifies what `seed/build_xwalk.py` already does; closes a
silent MAP-vs-TERRITORY divergence where this ADR's body + STTM claimed OBP was wired into Gold
while the code excluded it.** No schema change to any existing table — this is a
classification/doc reconciliation, made under the doctrine "what's deliberately out of the model
stays named, not silently absent" (Decision line ~43, journey/04 §identity).
- **Ruling**: Open Bank Project (`sil_obp_accounts`/`sil_obp_transactions`) terminates at Silver.
  It is NOT seeded into `dim_customer_xwalk`, NOT a golden-record survivorship tier, and feeds NO
  Gold BQ mart. The estate's cross-source customer identity resolves across the FOUR seedable
  sources (Berka/CRM, Home Credit, PaySim, Teradata) — OBP is the fifth, live-only, source that
  demonstrates REST-API medallion ingestion but is not conformed into the star.
- **Why Silver-terminal, not wired in (three independent disqualifiers, all verified on disk):**
  1. **No conformable customer identity → wiring in would FABRICATE it (R-38 red line).**
     `pipeline/extract/obp_client.py` (docstring lines 10-20) walks the PUBLIC accounts of ~199
     foreign demo banks (the `/my/accounts` owned-account endpoint is empty). These are
     institution-level public sandbox accounts, not customer records; no OBP `account_id`
     corresponds to a real/simulated person in our estate. Assigning one an existing `customer_id`
     would invent a relationship with no analog even as simulation — corrupting the one thing the
     MDM crosswalk exists to prove. (Contrast Teradata R-38: Bank Marketing IS a customer-attribute
     survey, so "this respondent is also our customer" is defensible; a foreign demo public account
     is not.)
  2. **Live sandbox breaks seed-time determinism (D-04).** `dim_customer_xwalk` is generated at
     seed time from static CSVs. OBP has no static CSV — it is a live walk with caps
     (`MAX_BANKS=25`/`MAX_ACCOUNTS=60`) over drifting sandbox contents. A non-replayable source
     cannot be a member of a deterministic seed-time conformed dimension.
  3. **FX-coverage gap.** OBP sandbox transactions are multi-currency (GBP/USD/…);
     `seed/artifacts/fx_rates.csv` covers only MYR/CZK/EUR/`unitless` (verified 2026-07-16).
     Routing OBP through `to_myr` would reintroduce the exact silent-mixing class R-14/D-12 just
     fixed (Addendum #1). BQ-08 liquidity is OUR bank's deposits/net flow anyway — foreign
     demo-bank public balances are not our liquidity.
- **Scope**: NOT scope creep and NOT scope loss — no locked BQ (BQ-01…BQ-10,
  journey/02_BUSINESS_QUESTIONS.md) names OBP in its definition-of-done; journey/03 lists OBP only
  as an ALTERNATIVE to Berka for balance/deposit/dormancy needs, and Berka alone satisfies each.
  Removing OBP from Gold breaks zero BQs. Any future OBP *analytics* value would be a NEW
  source-isolated fact/mart (grain: one row per OBP transaction, no `dim_customer` join) and must
  go through `governance/ADR/ADR-000-feature-intake-protocol.md` + `@scope-guardian` — it is not
  unlocked by this ruling.
- **Portfolio framing**: OBP's demonstrated skill (live REST ingestion — DirectLogin auth, token
  refresh-on-401, backoff + circuit-break, multi-endpoint walk, verbatim JSON Landing→Bronze→
  Silver) stands on its own at Silver. The senior signal is the *judgment* to NOT force an
  unconformable live source into the MDM star, not a fabricated join.
- **Docs reconciled to this ruling (2026-07-16, same commit):** `journey/05_STTM.md`
  (`obp_account_id` xwalk row + `core=2` survivorship re-scoped to "reserved, not populated in
  v1"), this ADR body (lines ~21/24 OBP native-key + survivorship-tier claims), `journey/04_DATA_
  MODEL.md` (OBP `account_id` = Silver-only native key), `journey/03_DATA_REQUIREMENTS.md`
  (BQ-01/06/07/08 rows — OBP not a Gold contributor in v1), `pipeline/gold/fact_txn.py` docstring
  (OBP named-out, was a dangling to-do), `seed/build_xwalk.py` `SOURCE_PRIORITY` (`obp` marked
  reserved/unused).

**Addendum #3 (2026-07-17, @staff-data-engineer sign-off) — `dim_customer_xwalk`'s CANONICAL
(full D-14 scale) artifact moves from a git-committed CSV to an S3 Delta artifact; the D-14
DEV-LOOP CSV stays git-committed, unchanged.** Not a grain or model change (still one row per
`(customer_id, source_system)`, journey/04_DATA_MODEL.md) — a load-path fix, triggered by a real,
live-caught BQ-09 finding (journey/08_SERVING_AND_EVIDENCE.md, 2026-07-17): the crosswalk's PaySim
leg was still capped at a 32,976-row D-14 dev-loop sample (`seed/build_xwalk.py
--paysim-sample`) while Bronze/Silver PaySim had been re-seeded to the real full 6,362,620-row
canonical population in the same session — so 99.67% of real `fact_txn` PaySim rows resolved to no
`customer_id` at all. journey/01_DATASET_AND_SOURCES.md line 59 (D-14) already mandates "full set
for the canonical run" — this addendum finishes that already-locked requirement for the crosswalk,
it does not reverse or amend D-14 itself.
- **Why a load-path change, not just "rerun with a bigger `--paysim-sample`"**: at real full
  PaySim scale (6,362,620 transactions, ~6.9M unique `C`-prefixed customer-shaped identifiers
  across `nameOrig`+`nameDest`, R-09 excludes `M`-prefixed merchants), the resulting
  `dim_customer_xwalk.csv` is ~278MB/7,236,379 rows. **No Git LFS is configured in this repo**
  (`git config --get filter.lfs.clean` returns nothing meaningful, no `.gitattributes`) and GitHub
  hard-rejects any single file over 100MB on a normal push — committing the full-scale artifact is
  not a style preference, it is mechanically impossible without adding new infra. Git LFS was
  considered and rejected: it would make git a data store for a Gold DIMENSION, doubling down on
  the underlying anti-pattern (a 250MB+ generated table belongs in the S3 data plane like every
  other Gold table, not in git) rather than fixing it.
- **Ruling**: the small D-14 dev-loop CSV (`seed/artifacts/dim_customer_xwalk.csv`, ~12MB/345,857
  rows) stays git-committed — same treatment as `dim_fx_rate`'s `fx_rates.csv` (Addendum #1), a
  small hand-authored/bounded artifact genuinely code-adjacent. The full D-14 canonical artifact is
  generated once locally (`seed/build_xwalk.py`'s memory-safe streaming build — see that file's
  own "Memory note" docstring, unrelated OOM fix, not a design change) and written to S3 as a Delta
  table under a Gold seed-artifact prefix (`banking/gold/_seed_artifacts/dim_customer_xwalk/`).
  `pipeline/gold/dim_customer_xwalk.py` now prefers this S3 artifact when it exists, falling back
  to the small git CSV otherwise — the dev loop (where no S3 artifact is ever written) is
  unaffected, unchanged behavior.
- **This does NOT touch the `git_source` "ship code, not data" boundary** (ADR-002 Add #6,
  ADR-008's rejected-alternative "DAB uploads pipeline source" row) — that boundary bans shipping
  PIPELINE CODE outside git; `dim_customer_xwalk` is Gold DATA (a conformed dimension, this ADR's
  own subject), which has always belonged in the S3 data plane. The git-committed CSV was only ever
  viable because dev-loop scale (12MB) made the anti-pattern invisible; the 278MB canonical size
  merely exposed a pre-existing latent issue, this addendum corrects it rather than introducing a
  new exception.
- **Blast radius, named not hidden**: every Gold table that joins through `dim_customer_xwalk`
  changes at canonical scale once this artifact is populated — `dim_customer`, `fact_txn`,
  `fact_card_fraud`, `fact_loan_application`, `dq_currency_gate`, and every mart that reads any of
  those (`mart_customer_360`, `mart_cross_sell`, `mart_dormancy`, `mart_daily_flows`,
  `mart_risk_segment`, `mart_pipeline_health`). All must be rebuilt and re-verified after the
  artifact lands — tracked in `journey/08_SERVING_AND_EVIDENCE.md`'s BQ-09 evidence row, cost
  routed through `@finops-agent` before the redeploy.

**Addendum #4 (2026-07-18, @staff-data-engineer model ruling + @scope-guardian volume sign-off) —
five NEW conformed Gold objects promote the Silver data the analytics marts consume, so the
dbt-on-Snowflake serving layer reads Gold ONLY, never Silver.** This is medallion-debt paydown
under ADR-003/ADR-005 completing the already-locked BQ-01..10 scope — NOT a new mart, NOT a new
BQ, NOT a security-boundary move. It closes a real latent smell surfaced by building the dbt
serving layer (`governance/plans/PLAN-dbt-marts-serving-layer.md`): 6 of the 8 analytics marts,
as authored in Spark, reach past Gold directly into Silver tables (`mart_customer_360`→
`sil_campaign_response`; `mart_fraud_followup`→`sil_crm_case`; `mart_loan_funnel`→`sil_application`
+`sil_previous_application`; `mart_risk_segment`→`sil_application`+`sil_campaign_response`;
`mart_cross_sell`→`sil_campaign_response`+`sil_disp`+`sil_trans`; `mart_daily_flows`→`sil_trans`).
Spark tolerates cross-layer reads; a Snowflake serving role cannot (`journey/09_SECURITY_AND_
ACCESS.md` line 53 "no analyst-facing role reading Silver directly", line 69 "`serving_ro` =
Gold external tables only"), and the specific Silver columns carry confidential/risk
classification (`journey/09_SECURITY_AND_ACCESS.md` lines 40-41: `credit_in_default`,
`job`/`marital`/`education`).

- **The vetoed alternative (recorded so it is not silently re-proposed):** exposing those 5 Silver
  tables as Snowflake external tables was VETOED by `@staff-data-engineer` — it would institutionalize
  the medallion layer-skip AND hand `serving_ro` row-level confidential/risk data, the exact
  PII-to-wrong-role threat `journey/09_SECURITY_AND_ACCESS.md` line 114 exists to prevent. The
  security boundary does not
  move; the data moves up to Gold where serving is allowed to read it.

- **The five new objects (grain / SCD / source):**
  1. `dim_campaign_response` — one row per `customer_id`; **Type 1 overwrite**; promoted 1:1 from
     `sil_campaign_response` (already 1:1 on `customer_id`, ADR-006 D6.2 assigns `customer_id` at
     seed, no native key). A DISTINCT conformed dimension keyed on `customer_id` — deliberately
     NOT folded into `dim_customer` (that stays the Type-1 golden-identity survivorship record;
     campaign response is a separate marketing-survey business entity — folding would be a
     mixed-domain dimension, doctrine violation). Its confidential/risk columns
     (`credit_in_default`, `job`, `education`) keep that classification AS a Gold table — becoming
     Gold does NOT launder RBAC; it stays scoped to BQ-05/06-facing roles, per `@scope-guardian`.
  2. `fact_crm_case` — one row per Salesforce Case; **no SCD** (append/snapshot event); from
     `sil_crm_case` with the Berka `client_id`→`customer_id` xwalk resolution done IN the Spark
     builder (so dbt never touches `dim_customer_xwalk` or Silver). Carries `case_id, customer_id,
     case_type, opened_at`.
  3. `fact_previous_application` — one row per `SK_ID_PREV`; **no SCD** (append event); from
     `sil_previous_application` + home_credit xwalk. Carries `sk_id_prev, customer_id, sk_id_curr,
     name_contract_status, days_decision`. (`fact_loan_application` already carries the CURRENT
     application's income/target/income-type — this covers only the distinct PRIOR-loan population
     `mart_loan_funnel`'s approval-rate proxy needs.)
  4. `fact_account_balance` — one row per `account_id`; **overwrite snapshot** (current-state,
     stated); materializes the existing `pipeline/gold/common.py` helper
     `latest_balance_per_account` as a real table. Carries `account_id, current_balance,
     current_balance_myr, currency`.
  5. `bridge_customer_account` — one row per `(customer_id, account_id)`; **Type 1 overwrite**;
     the customer↔account N:N as a BRIDGE table (not a CTE — `journey/04_DATA_MODEL.md` lines
     43-46 already name this pattern), from `sil_disp` + Berka xwalk. Carries `customer_id,
     account_id, relation_type`
     (OWNER/DISPONENT). Both disp types retained (the marts that consume it decide filtering).

- **What this does NOT change:** `journey/09_SECURITY_AND_ACCESS.md` — byte-for-byte unchanged
  (that unchanged boundary is the evidence Option B is the correct shape, not an omission).
  ADR-002's "Snowflake reads Gold external tables only" — unchanged. Existing facts/dims — none
  extended (`fact_loan_application` already sufficient). `dim_customer_xwalk`/Silver/masking stay
  Spark single-path (ADR-005 core). dbt's `sources.yml` grows from 7 external tables to 12, ALL
  still over `banking/gold/`.

- **Blast radius / reversibility:** MODERATE / HIGH. 5 additive Gold builders + 6 mart transforms
  re-authored (they are being ported to dbt regardless); nothing existing dropped until dbt
  reconciles to each retiring mart's current S3 numbers (PLAN Phase-2 gate, extended to the 5 new
  intermediates). Revert = drop 5 tables + git-revert builders. Cost of the 5 extra Gold builder
  runs + affected-mart rebuild routed through `@finops` in the same envelope as PLAN Step-3.

**Addendum #5 (2026-07-18, @staff-data-engineer grain ruling) — HC-1/BQ-11 ("repayment discipline
vs default"): ONE new customer-grain Gold fact, `fact_repayment_behavior`, fed by 3 new native-grain
Silver builders over REAL Home Credit child tables; answered by a new dbt view,
`mart_repayment_risk`.** This is `@scope-guardian`'s owner-authorized 11th business question
(`governance/BACKLOG.md` "Superseded deferrals" — supersedes 3 of 4 rows in the 2026-07-18 HC
Bronze→Silver deferral). BQ-11 extends BQ-05 (demographic risk) with a behavioral-risk signal: does
a customer's actual repayment behavior (late-payment %, underpayment %, days-past-due) predict
their Home Credit `TARGET` default? Uses `installments_payments`, `credit_card_balance`,
`pos_cash_balance` — REAL Kaggle Home Credit data, currently Bronze-only, zero fabrication (passes
the owner's data-authenticity bar, `PROJECT_STATUS.md` twelfth-session entry). `bureau_balance` (the
4th un-Silver'd HC table) stays OUT — different join path (`SK_ID_BUREAU`→`bureau`), not part of
this build, its own deferral row unaffected.

- **The vetoed alternative (recorded so it is not silently re-proposed):** a single native-grain
  Gold fact (originally proposed as `fact_installments`, or any unified fact spanning all three
  source grains) was VETOED. Two independent doctrine violations: (a) it would re-arm the exact
  BQ-04 fan-out bug class (`mart_loan_funnel.sql`'s own precedent — aggregate native-grain sources
  to report grain FIRST, never join raw multi-row-per-customer data) inside a Snowflake external-
  table VIEW scanning 13M+ rows per query, both a serving-cost anti-pattern and the wrong place to
  keep the anti-fan-out invariant; (b) `installments_payments` (installment-event grain),
  `credit_card_balance` (CC monthly-snapshot grain), and `pos_cash_balance` (POS monthly-snapshot
  grain) are three different grains and three different business entities — one fact spanning all
  three is a 1-table-1-grain-1-entity violation (line 47 above) even before the fan-out risk is
  considered. Native-grain child rows terminate at Silver; Spark (not dbt/Snowflake) does the
  fan-out-safe aggregation to customer grain, landing ONE derived Gold fact.

- **The three new Silver builders (native grain, content-quality gate only — ADR-003, no
  aggregation at this layer):**
  1. `sil_installments_payments` — one row per scheduled installment per previous-application.
  2. `sil_credit_card_balance` — one row per credit-card previous-application × month snapshot.
  3. `sil_pos_cash_balance` — one row per POS-cash previous-application × month snapshot.
  All three: **no SCD** (append/snapshot, natural-key MERGE — same class as `sil_card_txn`).
  `SK_ID_CURR` stays a plain FK column at Silver; the xwalk hop to `customer_id` happens at Gold,
  same pattern as `fact_previous_application.py`.

- **The new Gold fact:** `fact_repayment_behavior` — **one row per `customer_id`**; **overwrite
  snapshot** (no SCD, recomputed each run — same stated class as `fact_account_balance`, Addendum
  #4 item 4). A Kimball consolidated fact: three 1:N sources rolled into one customer-grain
  behavioral-risk profile, not a mixed-domain table (the entity is singular — "this customer's
  repayment behavior" — with three source inputs, same shape as `dim_customer` surviving many
  sources into one golden record). Representative columns (final list in STTM):
  `customer_id, installment_count, late_payment_rate, underpayment_rate, avg_days_late,
  cc_months_dpd, cc_avg_utilization, pos_months_dpd, max_dpd`. Measures are mixed-additivity (rates
  non-additive) — noted in STTM, not re-derived downstream.

- **Fan-out-safe aggregation strategy (the load-bearing part, stated so it can't drift):** in the
  ONE Spark builder for `fact_repayment_behavior` — (1) aggregate `sil_installments_payments`
  `groupBy(SK_ID_CURR)` → 1 row/customer; (2) aggregate `sil_credit_card_balance`
  `groupBy(SK_ID_CURR)` → 1 row/customer; (3) aggregate `sil_pos_cash_balance`
  `groupBy(SK_ID_CURR)` → 1 row/customer; (4) resolve `SK_ID_CURR → customer_id` via
  `dim_customer_xwalk` (`source_system='home_credit'`) AFTER aggregation (xwalk grain is 1:1, so
  order doesn't fan out either way, but resolving post-aggregation keeps the aggregation itself
  source-native); (5) `FULL OUTER JOIN` the three already-1-row-per-customer results on
  `customer_id` — cannot fan out, because no input to the join has more than one row per customer.
  **Invariant for any future reader**: never join two multi-row-per-customer sources (or either of
  them to `fact_loan_application`) at raw grain — collapse each 1:N source to 1-row-per-customer in
  its own aggregate first, join only pre-aggregated customer-grain results.

- **The BQ-11 answer, `mart_repayment_risk` — dbt view**, per the standing BQ-01..08 convention
  (views over Gold external tables, `governance/plans/PLAN-dbt-marts-serving-layer.md`). **One row
  per `customer_id`** (mirrors `mart_risk_segment`/BQ-05, the story it extends). Reads Gold ONLY:
  `fact_repayment_behavior` JOIN `fact_loan_application` (for `target_default`) — both already
  customer-grain, 1:1 join, no fan-out in the view, Silver boundary untouched (Addendum #4). Adds a
  14th `sources.yml` external table (`fact_repayment_behavior`) and a 9th dbt mart model.

- **Clean-ERD doctrine check:** 1 table = 1 grain = 1 entity — satisfied (3 Silver at native grain,
  1 Gold fact at customer grain, 1 view); the vetoed unified native-grain fact is the violation this
  ruling avoids. Bridge not CTE for N:N — **N/A, deliberately not introduced**: customer↔installment
  is 1:N aggregation, not N:N: a bridge table here would itself be wrong, named explicitly so none
  gets added. Serving = view never a duplicated table — `mart_repayment_risk` is a dbt view;
  `fact_repayment_behavior` is a modeled Gold intermediate (conformed, reusable), not a serving
  duplicate. One explicit SCD per table — stated per table above (all no-SCD/snapshot, consistent
  with ADR-005's v1 no-Type-2 stance).

- **What this does NOT change:** no existing table's grain or schema; `dim_customer`, the xwalk,
  and every BQ-01..10 mart are untouched (this extends the BQ-05 story without editing
  `mart_risk_segment`). `bureau_balance` stays Bronze-only, its own deferral unaffected.

- **Blast radius / reversibility:** LOW-MODERATE / HIGH. Fully additive: 3 Silver builders + 1 Gold
  builder + 1 dbt model + 1 `sources.yml` row. Revert = drop the new tables + git-revert the
  builders. Cost of the Gold aggregation run (13M+ installment rows) routes through `@finops`
  before the canonical run.
