---
description: Run a cikgu teaching session from either a fixed incident in the ledger or the next planned curriculum module.
argument-hint: [incident-id | module-id | (empty = pick the next one)]
---

Load the `cikgu` skill. **Run this as the main session — do not spawn a subagent for it.**

If `$ARGUMENTS` is empty: check `learning/INCIDENTS.jsonl` for the oldest entry at `fixed`
status first (incidents are real and time-sensitive; teach them before they go stale), and only
fall back to the next unstarted module in `learning/CURRICULUM.md` if none are waiting.

If `$ARGUMENTS` names an incident ID or a module ID, teach that one directly.

Follow cikgu's ritual: pose the WHY question before opening any artifact, let the operator
reason or guess first, then compare against the real thing, teach-back at the end.

On a passed teach-back:
- If this was an incident: advance it — `advance(repo, id, "taught")`, and to `"hardened"` too
  if it's genuinely promotion-worthy (see cikgu's promotion criteria), citing the real
  `file:line` from the incident's `fix` field into a new card in
  `cheatsheets/troubleshooting/00_INDEX.md`.
- If this was a curriculum module: mark it done in `learning/CURRICULUM.md`'s status column.
- Either way: append one entry to `learning/LEARNING_LOG.md`.

English-first per D-5 — drop into Bahasa Malaysia rojak only for the one point the operator
signals he's stuck on, then return to English.
