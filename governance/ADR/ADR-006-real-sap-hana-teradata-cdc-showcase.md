# ADR-006 — Owner override: real SAP HANA Cloud + Teradata replace SAP-sim; CDC-connector showcase

**Status:** Accepted (owner override of prior scope)
**Date:** 2026-07-06
**Owners:** owner (ratified), architect (sign-off)
**Context refs:** supersedes the SAP-sim mechanics in `ADR-003-four-layer-medallion.md` and narrows
`ADR-004-batch-first-cdc-later.md`'s "CDC is Fasa C, later" for two specific sources only;
`governance/BACKLOG.md` ("SAP BTP trial / ABAP Docker" — original rejection, now superseded for
a different reason than originally rejected).

## Context
The original design (CIL planning lab, `01_OPUS_DECISIONS.md` REJECTED list) rejected a real SAP
instance for two reasons: a 90-day trial wall, and 16-32GB RAM for a full SAP BTP/ABAP stack. The
owner's actual goal, stated directly in this build session, changes both premises:
1. **Trial-wall is explicitly a non-issue** for this owner's operating model — projects here are
   run "maybe max 3-4 times"; a new trial/free-tier account is created per future project as
   needed. This is the SAME reasoning already accepted for the Databricks trial (`ADR-002`, D-01
   Add #3's "disposable trial" operating model) — this ADR extends that accepted precedent to two
   more services rather than contradicting it.
2. **The heavy-RAM concern doesn't apply** — the owner is not standing up ABAP/Netweaver. **SAP
   HANA Cloud** (Free Tier, confirmed available in the owner's BTP account service marketplace) is
   a managed database service only — no ABAP environment, no Integration Suite/SLT/SDI middleware.
3. **The actual goal is learning CDC connector engineering as a data engineer** — not platform
   administration. This re-frames what "using SAP/Teradata" means here: we are not adopting SAP's
   or Teradata's proprietary replication middleware (that would be platform-engineer/DBA work);
   we are treating SAP HANA Cloud and Teradata as two genuinely different **real relational
   database targets** against which to build a portable, hand-written CDC-style extractor —
   the same category of skill as the existing Postgres/MSSQL watermark extractors, one level up.

## Decision

### D6.1 — Five source systems (was four)
| # | Source system | Role | Dataset | Mechanism |
|---|---|---|---|---|
| 1 | Open Bank Project sandbox | Core Banking API | none (sandbox-generated) | REST, unchanged |
| 2 | MS SQL Server (Docker) | Credit Card + Fraud | PaySim | batch watermark, unchanged |
| 3 | PostgreSQL (Docker) | Sales / Loan Dept | Home Credit Default Risk | batch watermark, unchanged |
| 4 | **SAP HANA Cloud (BTP Free Tier)** | Internal CRM | Berka (Czech Financial Dataset) | **CDC-style (D6.3)** — replaces the file-drop simulation |
| 5 | **Teradata** (Vantage Express or Teradata Cloud free tier) | Marketing / Campaign | **UCI "Bank Marketing" dataset** (D6.2) | **CDC-style (D6.3)**, new source |

Landing → Bronze → Silver → Gold (ADR-003) is UNCHANGED for all five — this amendment changes
WHERE two sources live and HOW they're extracted, not the medallion shape. The Landing→Bronze
promotion gate's transport-integrity job (dedup, `_SUCCESS`, schema-drift firewall) already
anticipated absorbing CDC events (ADR-003 point 6: "CDC future... Landing-as-transport-buffer
future-proofs the exact upgrade the project is built around") — this ADR simply exercises that
design two fasas earlier than v1 originally planned, for the two sources chosen as the
CDC-learning showcase.

### D6.2 — Teradata's dataset: UCI "Bank Marketing" (Moro et al., freely downloadable, no auth)
Verified directly (not assumed): `https://archive.ics.uci.edu/static/public/222/bank+marketing.zip`
returns HTTP 200 with no login — this also removes this one source from the Kaggle-credential
blocker entirely. ~45,211 rows: customer demographics (age, job, marital status, education),
financial signals (`default` — credit in default y/n, `balance` — avg. yearly balance, `housing`/
`loan` — existing product flags), and term-deposit campaign outcome (`contact`, `campaign`,
`pdays`, `previous`, `poutcome`, `y` — subscribed y/n).

**Named modeling problem, resolved explicitly (not silently assumed):** this dataset has **no
natural key** linking its rows to any of the other three sources — it's an independent Portuguese
bank-marketing survey, not a shared-customer export. Resolution: at **seed time**, deterministically
assign each Bank Marketing row to an existing `customer_id` from `dim_customer_xwalk` — a seeded
random sample **without replacement**, sized to `min(len(bank_marketing_rows), len(xwalk_customers))`.
This is the SAME kind of seed-time synthetic linkage the project already does for every other
source (D-04's crosswalk is itself a seed-time construct, not a discovered join key) — stated
explicitly here, not hidden, per the anti-shortcut "tag assumptions" rule. Rows beyond the xwalk
population size are dropped at seed (documented, counted, not silently truncated).

### D6.3 — CDC mechanism: generalized trigger + change-table pattern (portable, not platform-native)
For SAP HANA Cloud and Teradata specifically (Postgres/MSSQL stay on the original ADR-004 batch
watermark — unchanged):
- A `_cdc_log` shadow table per source table: `(op CHAR(1), pk_value, changed_at TIMESTAMP,
  seq BIGINT)` where `op` ∈ {I, U, D}, populated by a standard SQL trigger (`AFTER INSERT/UPDATE/
  DELETE`) on each source table — plain SQL DDL, available on any HANA Cloud/Teradata tier,
  requiring no proprietary replication middleware (SAP SLT/SDI, Teradata QueryGrid explicitly
  OUT — see Rejected alternatives).
- The extractor (pipeline/extract/cdc_common.py, shared logic; `sap_hana_extract.py` /
  `teradata_extract.py` as thin per-source drivers) polls `_cdc_log` ordered by `seq`, tracks its
  own offset (last-processed `seq`, stored in the lake like the batch watermark), and lands each
  poll's events into Landing as a `dt=YYYY-MM-DD` partition — same shape as every other Landing
  arrival.
- This is the correct level of authenticity for the stated goal: the extractor code demonstrates
  real CDC engineering (op-code handling, ordering, offset tracking, idempotent landing, dedup of
  a redelivered poll) — the transferable data-engineering skill — without requiring the platform-
  administrator skill of configuring a vendor's proprietary CDC/replication service.

### D6.4 — Business-question enrichment (no new BQ; three existing BQs gain a genuine new source)
Per owner instruction not to limit enrichment to a single BQ, the Bank Marketing fields map onto
**three** of the locked BQ-01…10 (chosen because each has a real, non-arbitrary field mapping —
not spread thin for its own sake):
| BQ | New signal from Teradata/Bank Marketing | Why this BQ specifically |
|---|---|---|
| BQ-01 (Customer 360) | term-deposit product flag (`y`) becomes one more product in the relationship-value count | Bank Marketing's `y` is literally "does this customer hold a term deposit" — a product BQ-01 already counts |
| BQ-05 (Default risk by segment) | `job`/`education` enrich the income-band/employment segment cut; `default` (credit-in-default flag) is a second, independent default signal alongside Home Credit's `TARGET` | Two independent default signals on the same customer_id is a genuine cross-source risk-reconciliation story, not decoration |
| BQ-06 (Cross-sell targets) | `poutcome`/`y` (prior campaign responsiveness) directly targets "who is likely to respond to a cross-sell offer" | This is the field's original real-world purpose — most direct fit |

No 11th BQ — scope stays at the locked 10 (`journey/02_BUSINESS_QUESTIONS.md`), per scope-guardian
discipline; three marts gain one more joined source, they don't gain new grain or new marts.

### D6.5 — New named risks (this repo's own register — R-36…R-39, not in the CIL planning lab's
R-01…R-35; tracked here since this ADR is this repo's own divergence from that upstream plan)
| ID | Risk | Resolution |
|---|---|---|
| R-36 | SAP HANA Cloud CDC: duplicate/out-of-order change events from the trigger/poll pattern | Same Landing→Bronze promotion gate dedup mechanism as every other source (ADR-003) — ordering via `seq`, dedup via `(pk_value, op, seq)` |
| R-37 | Teradata CDC: same risk class as R-36 | Same resolution, same shared `cdc_common.py` logic |
| R-38 | UCI Bank Marketing has no natural customer key | Seed-time deterministic sampled assignment to existing `dim_customer_xwalk` customer_ids (D6.2) — named, counted, not hidden |
| R-39 | SAP HANA Cloud / Teradata Free Tier network exposure misconfigured (no internet-facing endpoint enabled) | Operational risk, not a data risk — connection simply fails closed; documented in `journey/07_PIPELINE_SPEC.md` as a setup prerequisite, not a silent failure mode |

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| Keep the original file-drop SAP-sim, add Teradata as a 5th file-drop too | Doesn't teach the CDC-connector skill the owner explicitly asked for; the whole point of this override is real-system CDC practice |
| SAP Smart Data Integration / Landscape Transformation (SLT), Teradata QueryGrid | Platform-engineer/DBA configuration work, not data-engineer connector-writing — directly contradicts the owner's stated learning goal; also a much heavier setup that risks burning the session on plumbing before pipeline code exists (the same failure mode ADR-004 already named for "CDC-first") |
| Reuse Home Credit or Berka data again for Teradata (no new dataset) | Teaches only "how to connect to Teradata," not why a genuinely separate business system (marketing/campaign) exists in a real bank's estate — dilutes rather than reinforces the MDM/xwalk keystone story |
| Add an 11th BQ for "campaign responsiveness" | Owner explicitly asked for enrichment across 2-3 existing BQs, not a new one; keeps the locked 10-BQ scope honest |

## Consequences
- `01_DATASET_AND_SOURCES.md`, `04_DATA_MODEL.md`, `05_STTM.md`, `06_DQ_PLAN.md`,
  `07_PIPELINE_SPEC.md`, `09_SECURITY_AND_ACCESS.md`, and `gates/framework.yml` all need updates
  reflecting 5 sources instead of 4 — tracked as a single amendment commit, not scattered silently.
- `governance/BACKLOG.md`'s original "SAP BTP trial / ABAP Docker — REJECTED" row is **not deleted**
  (per the ADR-addendum-parity discipline — history stays visible); a new row records the owner
  override with today's date and the actual reason (trial-wall accepted as non-issue + HANA-Cloud-
  only, not full BTP/ABAP).
- Does NOT decide exact SAP HANA Cloud / Teradata instance sizing, or the literal DDL for the
  `_cdc_log` tables — that's `journey/07_PIPELINE_SPEC.md` / the actual seed scripts' job.
- Live testing against real SAP HANA Cloud / Teradata instances is blocked until the owner
  provisions them and supplies connection details (never credentials in chat — via `.env`/secret
  scope per `journey/09_SECURITY_AND_ACCESS.md` §1); until then, all code here is written against
  the local-disk/dev-loop fallback and is unverified against the live services (tagged as such in
  `BUILD_REPORT.md`).

## Addendum log
None yet.
