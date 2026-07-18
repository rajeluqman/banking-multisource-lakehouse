# Optimization Library — banking-multisource-lakehouse (INDEX)

> A **static catalog** of performance/cost techniques, one card per technique, each tied to a real
> layer of THIS pipeline. Cards are fed by real findings (an SLA/perf observation → 🟡 APPLICABLE
> → ✅ DONE once applied and cited). Content is English.

> ## 🚧 STATUS: STUB — grows on real findings only
> **This INDEX file *is* the entire optimization artifact for now.** It is NOT split into
> per-layer files yet — that would be premature sprawl.
> - **Gate:** a real card is authored only AFTER a real perf/cost finding exists to document.
> - **Owner:** @senior-data-engineer (perf/idempotency); @finops-agent watches metered cost
>   (Databricks / Snowflake / Kaggle-API).
> - **Authoring rule:** every ✅ card cites a real `file:line` from an actual fix. **No fabricated
>   findings, no invented citations, no speculative ✅ cards.** A 🟡 seed is a real, unapplied
>   technique — not a claim of work done.
> - **Split rule (lazy):** promote a card into its own `0N_<layer>.md` file only when that one
>   layer earns **≥3–4 real cards** — split on volume, never preemptively on taxonomy.

## How to use
1. Each layer holds cards. Fill one card per technique.
2. Classify every card: **✅ DONE** (applied + cited `file:line`) · **🟡 APPLICABLE** (real, not yet
   applied) · **⬜ N/A** (doesn't apply here — say why).
3. Every ✅ card MUST cite a real `path:line`. No fabricated citations.

## Card format (copy this)
```
### <ID> — <technique name>
- **Layer:** ingestion | bronze | silver | gold | serving | orchestration | dq | shared
- **Status:** ✅ DONE | 🟡 APPLICABLE | ⬜ N/A
- **What:** one line — the technique.
- **Why here:** why it matters for THIS workload (multi-source batch + Delta + star schema).
- **Applied at:** `path/to/file:LN` (✅ only) — or "not yet".
- **Junior mistake:** the trap this avoids.
- **Measured effect:** before → after (latency / $ / rows), if known.
```

## Layer map (planned files — none split out yet; see split rule above)
All layers are **⬜ gated · 0 cards** until a real finding trips the gate.

| # | Layer | Eventual file | Status | Cards | Focus for this project |
|---|-------|---------------|--------|-------|------------------------|
| 01 | Source→Landing | `01_ingestion.md` | ⬜ gated | 0 | watermark skip-existing, incremental extract, CDC-poll batching |
| 03 | Bronze | `03_bronze.md` | ⬜ gated | 0 | append-only Delta, partition by ingest date, avoid rewrite |
| 04 | Silver | `04_silver.md` | ⬜ gated | 0 | MERGE on business key, partition pruning, mask once |
| 05 | Gold | `05_gold.md` | ⬜ gated | 0 | star-schema join order, bridge-table N:N, incremental marts, xwalk broadcast |
| 06 | Serving | `06_serving.md` | ⬜ gated | 0 | Snowflake external-table scan cost, view-not-copy, result shaping |
| 07 | Shared / storage | `07_shared.md` | ⬜ gated | 0 | small-file compaction `OPTIMIZE`+`ZORDER`, file sizing, cluster minutes |

## Seed card
### OPT-GOLD-01 — Delta compaction (OPTIMIZE) before serving
- **Layer:** shared / gold
- **Status:** 🟡 APPLICABLE (real technique for this project, not yet measured here)
- **What:** run `OPTIMIZE` (+ `ZORDER` on the common filter key) on Gold Delta to compact the
  small-file explosion that append-style writes create.
- **Why here:** Snowflake external tables and DuckDB scan Gold S3 directly; thousands of tiny
  Parquet files make every read pay per-file open overhead. Compaction is only worth it once read
  frequency > write frequency — the exact boundary ADR-010 draws.
- **Applied at:** not yet.
- **Junior mistake:** `OPTIMIZE` on every write (pays compaction cost on a write-heavy table for
  no read benefit), or copying Gold into a serving table instead of a view.
- **Measured effect:** projected scan-cost reduction; not yet measured in this build.

---
See `../README.md` for the honesty contract. Interview drills source their answer from a card's
fields, never from recollection (`learning/EXECUTIVE_STORYTELLING_TEMPLATE.md`).
