---
description: Formal root cause analysis under the TWO-STRIKE protocol — halts paid execution, produces a postmortem.
argument-hint: [incident-id]
---

Load `de-diagnosis`. This command assumes TWO-STRIKE has already fired (same stage failed
twice, or a fix didn't hold) — if it hasn't, use `/de-fix` instead; this is heavier by design.

1. **Halt** — confirm no further paid execution (cluster run, cloud job) is in flight or about
   to be triggered before proceeding.
2. Delegate to the `incident-commander` agent for the classification (code / state /
   environment) and artifact-level verification steps.
3. Write a postmortem into `governance/` using this shape:
   - **What happened** (observable symptom, timeline)
   - **Root cause** (the actual defect, not the symptom)
   - **Why it took two attempts** (what the first "fix" actually addressed, and why it didn't
     touch the real cause)
   - **Blast radius** (every artifact the same bug class could have touched)
   - **Fix** (`file:line`)
   - **Guard** (what now prevents this class of bug — a new gate rule, a new test, an ADR)
4. Advance the ledger entry: `kind: two_strike`, `status: diagnosed` at minimum, `fixed` once
   the fix lands and is verified at the artifact level.
5. If the guard step reveals a locked ADR was actually wrong, write the addendum via
   `staff-data-engineer` — do not silently patch around a bad design decision.

A postmortem produced by this command is exactly the kind of artifact `/de-teach` turns into a
`hardened` troubleshooting card later — write it as if a future session will learn from it cold.
