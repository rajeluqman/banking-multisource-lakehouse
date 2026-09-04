---
name: de-governance
metadata:
  compatible_agents: [claude-code]
  tags: [governance, adr, journey, boundary-contract, stop-gate]
description: >
  How to use this project's ADRs, journey docs, boundary contract, and gates before touching a
  model, schema, or storage path. Load whenever an edit would matter to
  `gates/framework.yml`'s governed paths, or before ruling on scope/design/security. Use for
  "which ADR covers this", "am I allowed to add this import", "does this need an addendum",
  "is this in scope".
---

# de-governance

## STOP-GATE — before editing a model, seed, schema, storage path, or ingest script

1. **Open the governing doc first.** Grain/model -> `journey/04_DATA_MODEL.md` + the relevant
   ADR. Stack boundary -> `governance/BOUNDARY_CONTRACT.md` + `gates/framework.yml`. Scope ->
   `governance/BACKLOG.md`. Security/access -> `journey/09_SECURITY_AND_ACCESS.md`. A new
   feature/source -> `governance/ADR/ADR-000-feature-intake-protocol.md`.
2. **Validate before building downstream.** Run the gates:
   `python gates/journey_completeness.py`, `python gates/boundary_contract.py`,
   `python gates/doc_reference_contract.py`, `python gates/secrets_scan.py`. These are binding
   checks, not judgement calls.
3. **If a rule and the request conflict, STOP and surface it.** Cite the doc, escalate to
   `staff-data-engineer` (model/schema) or `scope-guardian` (scope). Do not re-litigate a
   locked decision silently — a real conflict needs an ADR addendum, not a workaround.

Enforced three ways: this instruction (soft), `.claude/hooks/governance_guard.py` (blocks edits
to governed paths per `gates/framework.yml`), CI (`.github/workflows/ci.yml` blocks the PR).
**Governance is code, not vigilance** — if any of these three ever stop being able to fail,
that is a bug in the framework, not a feature.

## ANTI-SHORTCUT PROTOCOL

1. **Read-before-touch** — never edit or assert about a file from memory; read it this turn.
2. **Enumerate, don't sample** — for "all N" tasks, get N from ground truth first, re-count
   after acting.
3. **Reconcile-before-done** — before saying done/fixed, restate the request as a checklist
   with evidence (`file:line` / command output) per item. No evidence means "unverified," not
   "done."
4. **Tag assumptions** — any load-bearing claim not checked this turn is marked "(unverified)".
5. **The planning docs are a MAP; the files on disk are the TERRITORY.** If a doc names
   something that isn't actually there, stop and surface it — do not improvise silently. This
   is the exact bug class that let `framework_template/` get cited for months in a real project
   before anyone checked it existed (see `gates/doc_reference_contract.py`'s own docstring).

## TWO-STRIKE rule

If the same pipeline stage fails TWICE, or a fix "succeeded" yet the symptom persists, STOP all
paid execution and invoke `incident-commander` before any further cluster run. Classify
code/state/environment, verify the last fix at the ARTIFACT level (never trust run SUCCESS),
enumerate the bug-class blast radius, reproduce for free, then exactly one paid run.

## Feature intake

A new mart, source, or feature is not "just a quick addition" — it goes through
`governance/ADR/ADR-000-feature-intake-protocol.md`. `scope-guardian` holds veto until that
protocol has run. See `/de-source` for the command version of this flow.
