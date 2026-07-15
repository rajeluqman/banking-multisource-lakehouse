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
  maps one bank-wide `customer_id` to each source's native key (`SK_ID_CURR`, `nameOrig`/
  `nameDest`, `client_id`+`account_id`, OBP `account_id`). This is the conformed dimension every
  fact joins through; 8 of the 10 locked business questions are impossible without it (proof that
  the MDM layer needs to exist, not decoration).
- **Golden-record survivorship**: source priority **CRM (Berka) > core (OBP) > loans (Home
  Credit) > cards (PaySim)**, latest `updated_at` as the tiebreak within a source (R-23/R-24).
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
