---
description: Cross-repo drift audit — which projects are missing the framework, on a stale version, or carrying a stale hook generation.
argument-hint: <path containing your repos>
---

Delegate to the `fleet-auditor` agent. Run:

```
python supervisor/audit.py <path-containing-repos>
```

(from the `de-forge` kit repo, or wherever `supervisor/audit.py` was installed).

Report the table as-is plus the OBSERVATIONS section — do not summarize away the per-repo
detail into prose. State coverage explicitly (roots requested/found/skipped, repos discovered)
before the table, per the fleet-auditor's coverage-honesty contract.

If a repo shows a stale or hardcoded hook generation, or is missing `.claude/` entirely and is
still an active project, that is a candidate for `supervisor/install.py` — but this command
only reports, it never installs. Installing is a separate, explicit action the operator
approves per repo.
