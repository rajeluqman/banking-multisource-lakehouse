---
name: de-pipeline
metadata:
  compatible_agents: [claude-code]
  tags: [medallion, idempotency, cdc, watermark, dedup, merge, grain]
description: >
  Medallion-layer boundaries, idempotency, watermark/CDC/incremental ingestion patterns,
  dedup, MERGE upserts, and grain discipline. Load for `/de-build` or `/de-source`, or any
  "how do I ingest this", "how do I dedup this", "what layer does this belong in" question.
---

# de-pipeline

## Layer boundaries

Landing (transient, short TTL) -> Bronze (permanent, append-only, verbatim) -> Silver (cleaned,
masked, deduped, MERGE-upserted) -> Gold (star schema, business-ready marts, one grain per
table). Never conflate transport-integrity checks (did we receive it completely and exactly
once — a Landing->Bronze gate) with content-cleansing checks (nulls, orphan FKs, decode logic —
a Bronze->Silver gate). Conflating them is a common design mistake worth naming explicitly.

## Ingestion pattern by source shape

| Source shape | Pattern |
|---|---|
| RDBMS, has a reliable updated_at | Watermark-incremental batch |
| RDBMS/warehouse, no reliable timestamp | CDC via trigger + change-log table, or platform-native CDC |
| CRM (Salesforce-shaped) | Change-tracking API, not full re-pull |
| REST API, paginated | Paginated extraction with reconciled row count vs API-reported total |

## Idempotency

A failed run must be safe to re-run without double-counting. Two concrete techniques:
checkpoint-resume (restart from the last completed unit, not from scratch — and an unreadable
checkpoint must fail loudly, never silently restart from zero), and MERGE-based upsert keyed to
the table's true business key (not an autoincrement surrogate) so re-ingesting the same source
file cannot double-count downstream.

## Dedup

Window-function dedup (`ROW_NUMBER() OVER (PARTITION BY <business_key> ORDER BY <recency>)`)
keyed to each table's TRUE business key — get the grain wrong here and every downstream mart
inherits the error silently.

## Entrypoint contracts

Check `gates/framework.yml`'s `boundary.entrypoint_guard` for this project's platform — e.g. a
Databricks git-sourced task treats `SystemExit(0)` as a task failure, so an entrypoint must do
`_rc = main(); if _rc != 0: raise SystemExit(_rc)`, never a bare `raise SystemExit(main())`.

## Grain discipline (Clean-ERD doctrine — `staff-data-engineer` enforces this)

One table = one grain = one entity. Bridge tables for N:N, never a CTE standing in for one.
Serving layer is a view, never a duplicated table. One explicit SCD strategy per table, stated,
not implied.
