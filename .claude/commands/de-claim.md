---
description: Bind a resume/interview claim to real code, evidence, and a teach-back in the Claim Ledger.
argument-hint: [claim text, or a claim ID to update]
---

Load `de-evidence`. Read `interview/CLAIMS.md` first — never assert its current content from
memory.

**New claim** (`$ARGUMENTS` is claim text, no existing ID matches):
1. Draft the one-line claim as it would read on a resume.
2. Search for the real code backing it — cite `file:line`, not "the pipeline handles this."
   If no such code exists yet, the claim doesn't get written as DEFEND-track; either build it
   first or file it as DROP with a note.
3. Check what evidence actually exists: was it run on real data? Is there a row-count
   verification? A screenshot? Check only boxes that are literally true right now.
4. Run a teach-back on the spot — explain the claim out loud, unaided. If it doesn't hold up,
   that's real signal: fix the understanding before fixing the checkbox.
5. Append the entry to `interview/CLAIMS.md` in the documented format, with `status: DEFEND`
   only if evidence + teach-back both actually passed — otherwise `status: DROP`.

**Update existing claim** (`$ARGUMENTS` is a claim ID like `CLAIM-004`): re-run steps 2-4
against current reality (code may have changed since the claim was written) and update the
fields — never just flip `status` to DEFEND without re-doing the check.

After writing, run `python gates/claim_ledger.py` to confirm the entry is mechanically valid —
not a substitute for the honesty in steps 3-4, just a floor under it.
