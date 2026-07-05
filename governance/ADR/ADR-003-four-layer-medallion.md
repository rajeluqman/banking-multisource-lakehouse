# ADR-003 — Four-layer medallion: Landing and Bronze are separate layers

**Status:** Accepted
**Date:** 2026-07-05
**Owners:** owner (ratified), architect (sign-off)
**Context refs:** `01_OPUS_DECISIONS.md` D-15 in the planning lab (full rationale +
alternatives); `03_DATASET_RISKS_AND_RESOLUTIONS.md` R-15/R-16/R-18/R-22/R-28.

## Context
Canonical medallion architecture folds Landing into Bronze (2 raw-ish layers become 1). This repo
deliberately splits them because S3 has no atomic rename (temp→rename is copy+delete, not atomic)
and because four heterogeneous sources each produce a distinct transport-completeness problem
(partial API pulls, duplicate SAP file drops, truncated pagination, redelivered CDC events later).

## Decision
Layers = **Landing → Bronze → Silver → Gold**.
- **Landing** = transient arrival zone (short TTL). Data lands exactly as it arrives, including
  partial pulls, duplicate/re-dropped files, truncated pagination. Messy by design.
- **Bronze** = permanent, trusted, complete raw archive. A partition promotes Landing→Bronze
  **only** after passing the promotion gate. Still raw (no business cleansing) but guaranteed
  transport-complete and exactly-once.
- **The Landing→Bronze gate judges TRANSPORT INTEGRITY ONLY**: `_SUCCESS` marker present,
  manifest/checksum match, pagination reconciled vs API-reported totals, dedup of redelivered/
  re-dropped files, multi-file set complete. Fail → quarantine in Landing; Bronze untouched;
  pipeline continues on last-good Bronze.
- **The Bronze→Silver gate judges CONTENT QUALITY ONLY**: orphan FKs, nulls, `birth_number`
  decode, fraud-flag semantics, etc. If content cleansing creeps into the Landing→Bronze gate,
  Bronze stops being raw truth — forbidden.

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| Canonical 3-layer (Bronze == Landing) | Loses the ability to isolate a broken/partial/duplicate arrival from the permanent archive; a bad API pull or double-delivered file would either corrupt Bronze or require ad-hoc cleanup logic buried inside the "raw" layer |
| Rely on object-store atomic rename for isolation | S3 has no atomic rename (copy+delete) — this trick, which works on a real filesystem, is NOT a safe substitute for an explicit promotion gate on S3 |
| Content-quality checks inside the Landing→Bronze gate | Would silently launder "raw" — Bronze must remain the verbatim record of what the source sent, including its bad-but-intact rows (quarantined separately at Bronze→Silver, R-03) |

## Consequences
- Landing is double-storage relative to canonical medallion, but transient (short TTL) so the
  cost is trivial; the promotion-gate mechanism (manifest + `_SUCCESS`) was needed for R-15
  regardless, so this mostly promotes existing logic to an explicit, defensible layer.
- Future CDC upgrade (Fasa C, later — see ADR-004) lands events in Landing exactly like everything
  else; the transport-integrity gate absorbs duplicate/out-of-order events, so Bronze-down is
  UNCHANGED when CDC replaces the batch extractors. This is the strongest argument for the split
  (D-15 point 6) — it future-proofs the exact upgrade the project is built around.
- Does NOT decide: Landing's exact TTL number (default 7 days, adjustable in journey/07) or the
  specific manifest file format (specified in `journey/07_PIPELINE_SPEC.md`).

## Addendum log
None yet.
