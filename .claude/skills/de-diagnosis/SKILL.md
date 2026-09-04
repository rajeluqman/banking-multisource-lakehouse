---
name: de-diagnosis
metadata:
  compatible_agents: [claude-code]
  tags: [rca, troubleshooting, two-strike, incident, ledger]
description: >
  Reading an error backward from symptom to root cause, the TWO-STRIKE protocol, and
  artifact-level verification. Load for `/de-fix`, `/de-rca`, or any "why did this break",
  "same bug again", "the fix didn't actually work" situation. Owns writing to
  learning/INCIDENTS.jsonl.
---

# de-diagnosis

## The one rule

**Never trust a run's SUCCESS status. Verify at the artifact level.** A job can exit 0 and
still have written wrong data, skipped a partition, or silently down-cast a type. The owner's
own real incident history includes exactly this: grain violations and arithmetic errors that
only surfaced at full data volume, and a silent schema down-cast that a green run did not flag.

## Backward trace

Start from the observable symptom (a stakeholder complaint, a monitor alert, a wrong number in
a report) and trace backward through the pipeline stages until you find where the value first
went wrong — not where it was first NOTICED wrong. Those are usually different stages.

## TWO-STRIKE

Same stage fails twice, or a "fix" leaves the symptom unchanged: STOP paid execution, escalate
to `incident-commander`. Do not attempt a third fix on your own judgment — that pattern is what
this rule exists to interrupt (see `de-governance`'s ANTI-SHORTCUT PROTOCOL for the fuller
citation of why).

## Writing to the ledger

Every diagnosis session should end with either:
- `_incident_ledger.advance(repo, incident_id, "diagnosed", root_cause="...")` if this was
  triggered by an existing `gate_block`/`hook_block`/`two_strike` entry, or
- a fresh `_incident_ledger.append(repo, kind="rca", ...)` if the incident wasn't caught
  mechanically (e.g. a stakeholder reported it, not a gate).

The `root_cause` field must be specific enough that `/de-teach` can turn it into a question —
"the join key had duplicates" not "data issue."

## Reference Files

| File | Read When |
|---|---|
| _(none yet — add a reference file only when a specific platform's diagnostic quirks (e.g. Databricks Spark UI, Delta `DESCRIBE HISTORY`) earn their own file from repeated real use)_ | |
