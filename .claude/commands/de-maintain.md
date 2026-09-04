---
description: Maintenance pass — compaction, cost, schema drift, backfill. Checks the optimization library first.
argument-hint: [what needs maintaining]
---

Before doing anything, check `cheatsheets/optimization/00_INDEX.md` for an existing card
covering this — don't re-derive an optimization technique that's already documented as DONE or
APPLICABLE here.

Common maintenance jobs and where they route:
- **Compaction / small-file problem** — Delta OPTIMIZE / Z-ordering / partition tuning. Check
  the optimization library first; log a `kind: perf` incident if this required real
  investigation, not just running a known playbook.
- **Cost creep** — escalate to `finops-agent` for an estimate before running anything at full
  volume.
- **Schema drift** — this should already be caught by the Landing->Bronze promotion gate
  (transport integrity, not content cleansing — see `de-pipeline`). If drift reached
  production undetected, that's a `de-rca`, not a routine maintenance task.
- **Backfill** — confirm idempotency holds before backfilling (re-running a stage for a past
  date range must not double-count downstream). If unsure, treat this as a `/de-build` question
  first.

Any maintenance action that turns out to be more involved than expected (i.e., it surprised
you) is worth a ledger entry — that surprise is exactly what `/de-teach` turns into a lesson.
