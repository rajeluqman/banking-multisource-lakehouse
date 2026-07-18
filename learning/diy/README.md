# learning/diy/ — DIY build tickets & student attempts

When the student must reproduce real code (a dbt model, a PySpark job, a SQL transform), cikgu
runs **DIY Build Mode**:

1. **Spec handoff** — cikgu writes `TICKET_<name>.md` here: goal, inputs, expected output,
   acceptance criteria, out-of-scope, definition-of-done. **WHAT, never HOW. No implementation.**
2. **Student builds** `<name>_diy.sql` or `<name>_diy.py` here, with a pattern-level cheatsheet
   at the elbow — never the answer key.
3. **Diff vs reference** — only when the student says "done", cikgu opens the real
   model/spec and compares line-by-line, quizzing WHY on every difference.

These are practice artifacts. They are NOT part of the pipeline and are NOT wired into any gate.
