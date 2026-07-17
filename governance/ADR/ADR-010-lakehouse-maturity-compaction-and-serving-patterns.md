# ADR-010 — Lakehouse maturity: table maintenance now, Iceberg as forward-path, analytical-vs-operational serving boundary

**Status:** Accepted
**Date:** 2026-07-17
**Owners:** owner (ratified), @staff-data-engineer (technical ruling, Opus)

## Context
Raised by the owner during a serving-layer design discussion (2026-07-17), pursuing a
concrete goal stated in the owner's own words: **mature enterprise design that is fast, not
vendor-locked, keeps a single source of truth, and is cost-efficient on a real company bill
(not merely trial-credit-efficient).** Three questions drove it:
1. Is our S3 + Delta + multi-engine architecture genuinely enterprise-mature, or portfolio
   show-off?
2. At petabyte/trillion-row scale, how do real banks solve the tension between query
   performance, avoiding vendor lock, keeping one source of truth, and cost — without those
   forcing a trade-off against each other?
3. Would writing Gold directly into Snowflake (bypassing S3) be better? And where do Apache
   Iceberg and Delta compaction fit?

This ADR records the rulings from that discussion so they are not lost as chat, and closes a
long-standing named gap (R-41, journey/06_DQ_PLAN.md) with a concrete decision.

**Grounding (verified on disk this session):** the pipeline is 100% Delta (58 `format("delta")`
call sites, zero Iceberg); no `OPTIMIZE`/`ZORDER`/compaction stage exists anywhere in
`pipeline/` (R-41 confirmed unbuilt). ADR-002 locks the stack: S3 is the sole source of truth,
Databricks and Snowflake both read it in place, neither owns it.

## Decision

**1. Reaffirm S3 as the sole physical home of Gold (facts, dims, AND marts-truth). Reject
writing Gold natively into Snowflake.** (@staff-data-engineer trade-off ruling, 2026-07-17.)
Copying the authoritative model into a proprietary warehouse store is the "warehouse-as-
lakehouse" anti-pattern: it is the mirror image of the "Databricks-managed storage" alternative
ADR-002 already rejected, it deletes the multi-engine "S3 neutral truth, both read in place"
portfolio thesis (ADR-002 line ~95), and at petabyte scale it means paying twice for storage
plus losing the ability to point cheaper engines at the data. The authoritative model is copied
**zero times**; it lives once on S3 in an open table format.

**2. Adopt table maintenance (compaction) as a real pipeline stage — closes R-41.** Delta's
native `OPTIMIZE` (file compaction) + `ZORDER BY` (multi-dimensional clustering) become a
scheduled maintenance stage. This is the single highest-ROI performance lever for external-table
/ BI reads over Gold, requires no format change and no new tool (Delta supports it natively), and
directly addresses the Power BI read-speed concern that motivated the whole discussion.
`ZORDER BY customer_id` on `fact_txn` is specifically called out: it turns a per-customer lookup
from a full 6.3M-row scan into reading only the 1–2 files holding that customer.

**3. Name Iceberg / Delta UniForm as the documented FORWARD-PATH, not a now-build.** Apache
Iceberg is the most engine-neutral open table format (Snowflake, BigQuery, Redshift, Trino, Spark
all read it natively); "Iceberg-managed tables" (e.g. Snowflake Iceberg Tables) give near-native
query speed while the data physically stays in the owner's own S3 bucket in open format — fast +
no-lock + spine-intact simultaneously, the "have your cake" resolution of the performance-vs-lock
tension. Delta UniForm (write Delta, expose Iceberg metadata alongside) is the low-friction bridge.
**This is recorded as the correct forward path with explicit migration triggers (below), NOT
built now** — a full Delta→Iceberg conversion is a *migration project* of its own (re-platforms
all 58 call sites, re-runs and re-verifies every layer, amends this stack), disproportionate for a
single-dev portfolio repo on disposable trial infrastructure. Demonstrating the *judgment* of when
Iceberg matters is a stronger senior signal than building it prematurely.

**4. Record the analytical-vs-operational serving boundary.** Two distinct query shapes exist:
- **Analytical serving (OLAP)** — whole-population, aggregate, periodic-refresh (e.g. "total
  transactions this month"). This is what all 10 locked BQs are, and what the medallion + Gold
  marts + Snowflake-external-table serving is built for.
- **Operational / decisioning serving** — single-entity, computed at decision time, needs current
  data (e.g. "this one customer's net cash flow over the last year, at the moment they apply for a
  car loan"). This is a genuinely different pattern; real banks serve it from a decisioning/
  operational store or a governed federation engine (Trino) reading the serving tier — **never**
  a BI tool (Power BI) DirectQuery-ing a source system (the anti-pattern ADR-006 already rejected).
  Its *real-time freshness* requirement is a pipeline-cadence problem (batch vs streaming), which
  Iceberg/compaction do NOT solve.

  **This operational/real-time-decisioning pattern is explicitly OUT of v1 scope** — batch-first
  (ADR-004), with real-time/streaming deferred to Fasa C (governance/BACKLOG.md). Named here so it
  is a deliberate boundary, not a silent absence. Any future operational-serving BQ goes through
  ADR-000 intake + @scope-guardian, not straight into the pipeline.

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| Write Gold natively into Snowflake (owner's Q3) | Warehouse-as-lakehouse anti-pattern: reverses ADR-002's load-bearing "S3 sole truth, neither engine owns it" clause; mirror of the already-rejected "Databricks-managed storage" alt; deletes the multi-engine portfolio thesis; pays 2× storage + loses cheap-engine access at scale; HIGH blast-radius / LOW reversibility (breaks the Bronze→Silver→Gold spine, re-thrashes the just-relocated xwalk). Would require amending the ADR-002 *Decision itself*, not a mere addendum. |
| Full Delta→Iceberg migration now | Right knowledge, wrong time/scale/infra. A format re-platform of all 58 call sites + full re-run/re-verify on a disposable trial cluster, amending a locked stack, for marginal portfolio gain over documenting the forward-path. Over-engineering; the junior tell. Revisit per the triggers below. |
| Leave R-41 unbuilt (status quo, pure external tables, no maintenance) | Directionally correct but *unfinished* — small-files + no pruning make external-table/BI reads slow. This is the "stuck at option #2" state; compaction is what finishes the lakehouse into a fast, cheap, lock-free serving story. |
| Power BI DirectQuery against a source (e.g. Teradata) for per-customer decisioning | ADR-006 anti-pattern (federating a live analytical/decisioning query against an OLTP source). Also factually mismatched in this repo — the transaction data for such a query lives in MSSQL/Salesforce, not Teradata (source #5 = Bank Marketing survey only). |

## Consequences
- **Locks in**: S3 + Delta remains the authoritative Gold store; a compaction/maintenance stage
  becomes part of the pipeline's definition-of-done; Snowflake stays a read-in-place serving
  veneer (external tables + optional non-authoritative dbt views per PLAN-dbt-marts-serving-layer,
  never a physical copy of truth).
- **Makes easier later**: an Iceberg/UniForm adoption when justified — the maintenance discipline
  and the neutral-truth boundary are exactly what a future migration builds on.
- **Iceberg migration triggers (revisit this ADR when ANY holds):** (a) the platform becomes a
  real long-lived system (not a portfolio repo on trial infra); (b) three or more query engines
  genuinely need to read the same Gold at near-native speed; (c) a concrete Power-BI/DirectQuery
  performance SLA is unmet after compaction + Import-mode are exhausted; (d) a Snowflake-native
  consumer requires Iceberg-table performance over data that must stay in S3. At that point run a
  fresh ADR-000 intake for the migration project — do not treat this ADR as pre-approval.
- **Does NOT decide**: the compaction schedule/cadence (a build detail for @senior-data-engineer,
  bounded by the external orchestrator per ADR-007/D-10, not an in-repo scheduler); whether the
  dbt serving marts are views or selectively materialized (PLAN-dbt-marts-serving-layer, per-model
  call); any operational/real-time-decisioning capability (out of v1, Fasa C, ADR-004 + BACKLOG).

## Addendum log
(none yet)
