# Plan — {{feature/change name}}

**Status:** draft / approved / in-progress / done
**Governing ADR(s):** {{cite by number — every approved plan must cite at least one, even if
it's ADR-000 itself for a trivial change}}

## Goal
One or two sentences. What business question (journey/02) or requirement does this serve?

## Unresolved questions
(Per ADR-000 step 2 — must be populated or explicitly "none, because X" before this plan is
marked approved.)
- Q: ...? → A: ... (owner, date)

## Phases
| Phase | Output | Validation gate | Status |
|---|---|---|---|
| 1 | | | |
| 2 | | | |

Each phase should end with: run the relevant `gates/*.py` contracts, `git diff` review, commit,
then update this table's Status column before moving to the next phase.

## Rollback
If this needs to be reverted, what's the blast radius and the rollback step?
