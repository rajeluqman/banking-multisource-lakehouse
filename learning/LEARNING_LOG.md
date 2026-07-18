# LEARNING_LOG — cikgu teaching track

> Append-only. cikgu writes ONE entry per interaction (newest at the bottom). On resume, cikgu
> reads the last 3 entries + the current module in `CURRICULUM.md` — that is its entire memory.
> Do NOT edit past entries; they are the audit trail of what was actually taught.

Entry format:
```
[YYYY-MM-DD HH:MM]
Module: <curriculum module, e.g. M4 MDM crosswalk>
Question: <what the student asked>
Concept: <the tool-agnostic idea they were learning>
Hint level: <minimal | moderate | extensive>
Refs: <ADR / journey / model paths actually opened>
Score impact: -X
Next step when resuming: <one line — the resume checkpoint>
```

---

[2026-07-18 00:00]
Module: M0 Orientation
Question: (seed entry — no lesson yet)
Concept: cikgu learning track initialized. Curriculum grounded in real ADRs/journey docs.
Hint level: n/a
Refs: learning/CURRICULUM.md
Score impact: 0
Next step when resuming: Student picks a module (`@cikgu teach me M<n>`). Start at M0 if unsure.
