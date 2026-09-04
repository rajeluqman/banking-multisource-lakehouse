---
name: de-evidence
metadata:
  compatible_agents: [claude-code]
  tags: [claim-ledger, definition-of-done, receipts, integrity]
description: >
  Claim Ledger mechanics, project Definition of Done, and the integrity rule that a claim needs
  evidence and a passed teach-back before it may read DEFEND. Load for `/de-claim` or when
  closing out a project module. No prior art for this mechanism existed anywhere checked during
  design — this is original to this framework.
---

# de-evidence

## Definition of Done (project module)

A module is done only when all four hold, independently — any one alone is gameable:

1. **Runs** — the pipeline stage completes end-to-end on real data, not a sample fixture.
2. **Proves** — tests and the DQ suite pass in CI, not "it worked on my machine."
3. **Documents** — a runbook or RCA exists; any incident this module produced reached
   `hardened` in the ledger, not just `fixed`.
4. **Explains** — teach-back passed with no notes open. This is the gate that makes it a
   *taught* project, not one Claude built for the operator.

## Claim Ledger (`interview/CLAIMS.md`)

One entry per resume/interview claim. `gates/claim_ledger.py` enforces: `status: DEFEND`
requires at least one checked evidence box AND a checked teach-back box AND every `code:`
citation resolving on disk. Missing any of those means `status: DROP` — the claim does not ship
on the resume until it's actually earned.

Do not mark a box checked because the claim "should" be true. The gate checks that boxes are
checked and citations resolve; it cannot check that you were honest filling them in — that part
carries the same integrity weight as `defend-or-drop.md` / `integrity-rules.md` if this project
sits under a broader interview-prep system. A fabricated DEFEND is worse than an honest DROP.

## Workflow (`/de-claim`)

1. Draft the claim as it would read on a resume — one line, specific.
2. Find the real code citation(s) — `file:line`, not "somewhere in the pipeline."
3. Gather evidence — did you actually run it on real data? Verify a row count? Have a
   screenshot? Check only the boxes that are true.
4. Pass a teach-back — explain the claim out loud, unaided, to whoever is running the session.
5. Only then set `status: DEFEND`. Otherwise, `status: DROP` and move on — a dropped claim
   costs nothing; a fabricated one costs the interview.
