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
