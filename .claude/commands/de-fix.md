---
description: Troubleshoot a failing pipeline stage or a reported error, starting from the incident ledger.
argument-hint: [incident-id or symptom description]
---

Load the `de-diagnosis` skill first.

If `$ARGUMENTS` names an existing incident ID (e.g. `INC-0012`), read it from
`learning/INCIDENTS.jsonl` and continue from there. Otherwise, treat `$ARGUMENTS` (or the
current error context) as a new symptom and create a fresh incident:

```
python -c "
import sys; sys.path.insert(0, 'gates')
from pathlib import Path
from _incident_ledger import append
r = append(Path('.'), kind='rca', symptom='$ARGUMENTS')
print(r['id'])
"
```

Then:

1. **Backward-trace** from the symptom to the stage that first produced the wrong value — not
   the stage where it was first noticed. State the trace explicitly, one hop at a time.
2. **Verify at the artifact level** — read the actual output of the suspect stage. Never accept
   a green run status as proof anything is correct.
3. If this is the SAME stage failing a SECOND time, or a prior "fix" left the symptom
   unchanged — STOP. This is TWO-STRIKE. Hand off to the `incident-commander` agent instead of
   attempting a third fix yourself.
4. Once you have a real root cause, advance the incident:

```
python -c "
import sys; sys.path.insert(0, 'gates')
from pathlib import Path
from _incident_ledger import advance
advance(Path('.'), '<INC-ID>', 'diagnosed', root_cause='<specific root cause>')
"
```

5. After applying the actual fix, advance again to `fixed`, citing the real `file:line`:

```
python -c "
import sys; sys.path.insert(0, 'gates')
from pathlib import Path
from _incident_ledger import advance
advance(Path('.'), '<INC-ID>', 'fixed', fix='path/to/file.py:42 - <what changed>')
"
```

Do not close this out as "done" without that `fixed` advance — an untracked fix is a fix the
teaching layer never sees.
