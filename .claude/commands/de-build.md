---
description: Build a pipeline stage against an already-approved design, hybrid teaching mode (D-4).
argument-hint: <stage/table to build>
---

Load `de-pipeline`. Delegate to the `senior-data-engineer` agent for the actual build.

Before writing anything, confirm the design is settled: `journey/04_DATA_MODEL.md` or a
relevant ADR should already answer "what is this and why." If it doesn't, stop — this is a
`staff-data-engineer` design question, not something to improvise mid-build.

**Teaching mode (D-4 — hybrid by risk):**
- Boilerplate (scaffolding, config, repeated patterns already established elsewhere in this
  project): write it directly, don't make the operator retype a known shape.
- Core logic (transformation, dedup, incremental/watermark handling, error handling): use
  Plan-in-Comments-Then-Fill — write the steps as comments first, let the operator implement
  each one, then review the implementation against the plan and against `de-pipeline`'s
  patterns (grain discipline, idempotency, the entrypoint contract).
- Debugging during the build: Socratic first — ask what the operator expects to happen and why,
  before revealing what's actually wrong.

On completion, run the relevant gates (`boundary_contract`, `secrets_scan` at minimum) before
calling the stage done. If a gate blocks, that's now a ledger entry automatically — work it via
`/de-fix`, don't route around the hook.
