# Troubleshooting Library — banking-multisource-lakehouse (INDEX)

> Failure-path twin of the optimization library. One card per failure mode, per phase. Symptom
> presented FAR from root → trace backward (observability-first). Content is English.

> ## 🚧 STATUS: STUB — grows on real incidents only
> **This INDEX file *is* the entire troubleshooting artifact for now.** It is NOT split into
> per-phase files yet — that would be premature sprawl.
> - **Gate:** a real incident card is authored only AFTER a real incident is hit during the build.
> - **Owner:** @senior-data-engineer (build/diagnosis); @staff-data-engineer is Incident
>   Commander under the TWO-STRIKE rule (ADR-009) for any stage failing twice.
> - **Authoring rule:** every ✅ HARDENED card cites a real `file:line` from the actual fix.
>   **No fabricated incidents, no invented citations.** A 🟡 APPLICABLE seed is a real, undrilled
>   pattern — NOT a claim that an incident happened.
> - **Split rule (lazy):** promote cards into their own `0N_<phase>.md` file only when that one
>   phase earns **≥3–4 real cards** — split on volume, never preemptively on taxonomy.

## Binding reality note (read first)
This project **does** run Databricks portable PySpark + Delta (unlike a DuckDB-only stack). So
generic Spark advice applies directly — but two things are specific to THIS platform:

| Generic DE troubleshooting | This project's reality |
|----------------------------|------------------------|
| "Check the Spark UI / stage timeline" | Databricks Spark UI + Delta history (`DESCRIBE HISTORY`); local Spark repro needs `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` |
| "Executor OOM / shuffle spill" | `spark.sql.shuffle.partitions` sizing; skew on the MDM join key |
| "Run succeeded, data looks off" | **Never trust run SUCCESS — verify at the ARTIFACT level** (read the Delta output). ADR-009. |
| "No shared key across tables" | the MDM crosswalk (`dim_customer_xwalk`, D-04) — the identity layer is the #1 suspect for count anomalies |
| Same stage fails twice | **STOP paid execution — TWO-STRIKE rule (ADR-009).** @staff-data-engineer as Incident Commander before any further cluster run. |

## Card format (copy this)
```
### <ID> — <symptom, far from root>
- **Phase:** triage | ingestion | extraction | transformation | load | validation | orchestration | cicd | postmortem
- **Status:** ✅ HARDENED (fix cited) | 🟡 APPLICABLE (real, undrilled)
- **Symptom (business/observability):** what a stakeholder/monitor sees first.
- **Backward trace:** observable → … → root.
- **Root cause:** the actual defect.
- **Fix / guard:** `path/to/file:LN` (✅ only).
- **Junior mistake:** the wrong first move.
```

## Phase map (planned files — none split out yet; see split rule above)
All phases are **⬜ gated · 0 cards** until a real incident trips the gate. The first card for any
phase is authored inline under "Seed card" below, in this single doc.

| File | Phase | Status | Cards | Example failure modes for this project |
|------|-------|--------|-------|----------------------------------------|
| `01_triage.md` | Triage | ⬜ gated | 0 | "Customer-360 mart empty" / "fraud BQ returns nothing" — where to look first |
| `03_ingestion.md` | Source→Landing | ⬜ gated | 0 | watermark not advancing, CDC-poll `_cdc_log` gap, 0-byte export, connection creds |
| `05_transformation.md` | Silver/Gold | ⬜ gated | 0 | MERGE key skew, FK orphan fact, one customer counted as four (xwalk miss) |
| `06_validation.md` | DQ | ⬜ gated | 0 | gate flapping, quarantine filling, mask leak of PII into Gold |
| `07_orchestration.md` | Orchestration | ⬜ gated | 0 | skip-existing not firing, re-run non-idempotent, partial batch |
| `09_postmortem.md` | Postmortem | ⬜ gated | 0 | two-strike incident write-up (ADR-009) |

## Seed card
### TS-XWALK-01 — "Customer-360 shows more customers than the bank actually has"
- **Phase:** transformation
- **Status:** 🟡 APPLICABLE (real pattern for this project, undrilled)
- **Symptom:** the Customer-360 mart reports a customer count higher than any single source, and
  some "customers" have activity in only one system.
- **Backward trace:** inflated count → Gold facts FK to distinct `customer_id`s that are really
  the SAME human → `dim_customer_xwalk` failed to resolve that identity across sources → the four
  sources share no common key, so a fuzzy/deterministic match rule missed a link.
- **Root cause:** identity resolution gap in the MDM crosswalk — one human became N customers.
- **Fix / guard:** strengthen the resolution rule in `seed/build_xwalk.py`; re-derive the xwalk at
  real full scale (this actually happened once — see the BQ-09 rebuild in git history).
- **Junior mistake:** trusting the raw per-source customer IDs as if they were global keys.

---
See `../README.md` for the honesty contract. Interview drills source their answer from a card's
fields, never from recollection (`learning/EXECUTIVE_STORYTELLING_TEMPLATE.md`).
