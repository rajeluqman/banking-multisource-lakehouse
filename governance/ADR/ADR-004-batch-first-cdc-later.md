# ADR-004 — Batch-first ingestion; CDC is a later fasa, not v1

**Status:** Accepted
**Date:** 2026-07-05
**Owners:** owner (ratified), architect (sign-off)
**Context refs:** `01_OPUS_DECISIONS.md` D-02 in the planning lab; R-25/R-26.

## Context
The project's drill prep (`../control_plane_lab/saboteur/PROBLEM_BANK_OPTIMIZATION.md`) explicitly
stages a later CDC-vs-full-dump exercise. Building CDC (Debezium/Kafka or platform-native) first
would burn early build time on streaming plumbing before any of the portfolio-worthy transform
logic (MDM crosswalk, birth_number decode, survivorship, reconciliation) exists.

## Decision
Fasa B = high-watermark incremental extraction (`WHERE updated_at > :watermark`), with an overlap
window (watermark − 5 min) to catch late/out-of-order drip-feed writes (R-26), deduped on
PK + `updated_at` at the Silver MERGE. CDC is explicitly Fasa C, later — swaps the extractor only;
the Bronze contract and everything downstream (Silver MERGE, crosswalk, marts) does not change.

**Honest, named limitation (R-25)**: pure watermark batch cannot see hard deletes — a physically
deleted source row never reappears in a `WHERE updated_at >` query. Fasa B implements soft-delete
(`is_deleted` flag + `updated_at` touch) as the seed-side mitigation; true hard-delete capture is
out of scope until the Fasa C CDC upgrade (`op='d'` from the change log).

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| CDC (Debezium+Kafka) from day one | Burns early effort on Kafka/offset/schema-registry plumbing before any transform logic exists; batch and CDC produce identical Silver/Gold, so ~90% of the project's value is provable on batch alone |
| Full-dump-every-run (no watermark) | Defeats the purpose of the O-DSN-04/05 drill prep and does not scale to PaySim's 6.3M rows without hammering the source every run (R-10) |
| Ignore hard deletes entirely, undocumented | Rejected — an unstated gap is a bug waiting to be discovered by an interviewer; naming it here and in `journey/07_PIPELINE_SPEC.md` turns it into a demonstrated understanding of batch's limits instead |

## Consequences
- Silver/Gold correctness for inserts/updates is fully provable on batch alone; delete-completeness
  is explicitly NOT claimed until Fasa C ships — `journey/08_SERVING_AND_EVIDENCE.md` must not
  claim delete-handling that doesn't exist yet.
- Upgrade path is small by design (`.read` → `.readStream`; watermark state → CDC offset state) —
  this is itself part of the interview story, not just an implementation detail.

## Addendum log
None yet.
