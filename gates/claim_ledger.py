#!/usr/bin/env python3
"""Claim ledger gate — new in this framework (SPEC §2.3.1, §8.9), confirmed no prior art in any
of the 9 reference repos checked during design (RESEARCH_nasrul-crosscheck.md, D-15).

Parses `interview/CLAIMS.md` and enforces the rule that gives `defend-or-drop.md` teeth: a
claim may only carry `status: DEFEND` when it has BOTH at least one checked evidence box AND a
checked teach-back box. No evidence or no teach-back means the claim must read `DROP` — it
cannot ship on the resume. This is Definition-of-Done gate 4 (SPEC §8.9) made mechanical for
the specific case of an interview-facing claim rather than a project module.

Also checks, when `status: DEFEND`, that every `file:line` citation in the `code:` field
resolves on disk — an unverifiable citation is worse than an honest "no evidence yet" DROP,
because it looks defensible until someone actually asks to see it.

CLAIMS.md format (see `interview/CLAIMS.md` template for the full shape):

    ## CLAIM-001
    - **text:** "one-line claim"
    - **code:** path/to/file.py:12-30, other/file.py
    - **decision:** ADR-005 (or "none")
    - **evidence:** [x] ran on real data  [ ] row count verified  [ ] screenshot
    - **test:** path/to/test.py (or "none")
    - **explain:** [x] teach-back passed
    - **status:** DEFEND

Exit 0 = every DEFEND claim has evidence + teach-back + resolvable citations, or the file
doesn't exist yet (nothing to check).

Run:  python gates/claim_ledger.py
      python gates/claim_ledger.py --self-test
"""

from __future__ import annotations

import sys as _sys  # stdout/stderr reconfigure must happen before any print() call below.
# Both streams, not just stdout: a gate's failure path prints via file=sys.stderr, and a
# cp1252-decoded bullet byte re-encoded as utf-8 downstream (e.g. by a parent subprocess
# capture) produces mojibake in the incident ledger's symptom field — found by
# governance_guard.py's own smoke test, not by inspection.
for _stream in (_sys.stdout, _sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

import re
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CLAIMS_RELATIVE_PATH = "interview/CLAIMS.md"

CLAIM_HEADER = re.compile(r"^##\s+(CLAIM-\S+)")
FIELD = re.compile(r"^-\s+\*\*(\w+):\*\*\s*(.*)$")
CHECKED_BOX = re.compile(r"\[x\]", re.IGNORECASE)
ANY_BOX = re.compile(r"\[[ xX]\]")
FILE_LINE = re.compile(r"([\w./\\-]+\.\w+)(?::(\d+)(?:-\d+)?)?")


def _parse_claims(text: str) -> list[dict]:
    claims: list[dict] = []
    current: dict | None = None
    for line in text.splitlines():
        m = CLAIM_HEADER.match(line.strip())
        if m:
            if current:
                claims.append(current)
            current = {"id": m.group(1), "line": line}
            continue
        if current is None:
            continue
        fm = FIELD.match(line.strip())
        if fm:
            current[fm.group(1)] = fm.group(2).strip()
    if current:
        claims.append(current)
    return claims


def _citations_in(code_field: str) -> list[tuple[str, str | None]]:
    return [(m.group(1), m.group(2)) for m in FILE_LINE.finditer(code_field)]


def check(claims_text: str, repo: Path = REPO) -> list[str]:
    errors: list[str] = []
    claims = _parse_claims(claims_text)

    for c in claims:
        cid = c["id"]
        status = c.get("status", "").strip().upper()

        if status not in ("DEFEND", "DROP"):
            errors.append(f"{cid}: status must be DEFEND or DROP, got '{c.get('status', '<missing>')}'")
            continue

        if status == "DROP":
            continue  # a dropped claim has no further requirements — that's the point

        # status == DEFEND from here
        evidence_field = c.get("evidence", "")
        if not ANY_BOX.search(evidence_field):
            errors.append(f"{cid}: status=DEFEND but 'evidence' field has no checkboxes at all — malformed")
        elif not CHECKED_BOX.search(evidence_field):
            errors.append(f"{cid}: status=DEFEND but no evidence checkbox is checked — must be DROP until it is")

        explain_field = c.get("explain", "")
        if not CHECKED_BOX.search(explain_field):
            errors.append(f"{cid}: status=DEFEND but teach-back ('explain') is not checked — must be DROP until it is")

        code_field = c.get("code", "")
        if not code_field or code_field.lower() == "none":
            errors.append(f"{cid}: status=DEFEND but 'code' field is empty/none — a claim needs a citation to defend")
        else:
            for path_str, _ in _citations_in(code_field):
                if not (repo / path_str).exists():
                    errors.append(f"{cid}: status=DEFEND cites '{path_str}' but that path does not exist on disk")

    return errors


def self_test() -> bool:
    """Plant four claims: a properly-evidenced DEFEND (pass), a DEFEND with no checked evidence
    (fail), a DEFEND with evidence but no teach-back (fail), a DEFEND citing a nonexistent file
    (fail), and a DROP with nothing filled in (pass, since DROP has no requirements)."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "seed").mkdir()
        real_file = root / "seed" / "build_xwalk.py"
        real_file.write_text("SOURCE_PRIORITY = {}\n")

        claims_md = f"""# Claim Ledger

## CLAIM-001
- **text:** "properly evidenced claim"
- **code:** seed/build_xwalk.py:1
- **decision:** ADR-005
- **evidence:** [x] ran on real data  [ ] row count verified
- **test:** none
- **explain:** [x] teach-back passed
- **status:** DEFEND

## CLAIM-002
- **text:** "no evidence checked"
- **code:** seed/build_xwalk.py:1
- **decision:** none
- **evidence:** [ ] ran on real data  [ ] row count verified
- **test:** none
- **explain:** [x] teach-back passed
- **status:** DEFEND

## CLAIM-003
- **text:** "evidence but no teach-back"
- **code:** seed/build_xwalk.py:1
- **decision:** none
- **evidence:** [x] ran on real data
- **test:** none
- **explain:** [ ] teach-back passed
- **status:** DEFEND

## CLAIM-004
- **text:** "cites a file that does not exist"
- **code:** seed/nonexistent_file.py:1
- **decision:** none
- **evidence:** [x] ran on real data
- **test:** none
- **explain:** [x] teach-back passed
- **status:** DEFEND

## CLAIM-005
- **text:** "honestly dropped, nothing filled in"
- **code:** none
- **decision:** none
- **evidence:** [ ] ran on real data
- **test:** none
- **explain:** [ ] teach-back passed
- **status:** DROP
"""
        errors = check(claims_md, repo=root)

    checks = {
        "CLAIM-001 (proper) did NOT fire": not any("CLAIM-001" in e for e in errors),
        "CLAIM-002 (no evidence) fired": any("CLAIM-002" in e for e in errors),
        "CLAIM-003 (no teach-back) fired": any("CLAIM-003" in e for e in errors),
        "CLAIM-004 (bad citation) fired": any("CLAIM-004" in e for e in errors),
        "CLAIM-005 (DROP) did NOT fire": not any("CLAIM-005" in e for e in errors),
    }
    ok = all(checks.values())
    print(f"self-test: {len(errors)} error(s)")
    for name, passed in checks.items():
        print(f"   {'✓' if passed else '✗'} {name}")
    print("SELF-TEST PASS" if ok else "SELF-TEST FAIL — gate is hollow or over-firing")
    return ok


def main() -> int:
    if "--self-test" in sys.argv:
        return 0 if self_test() else 1

    path = REPO / CLAIMS_RELATIVE_PATH
    if not path.exists():
        print("✅ claim ledger OK (no interview/CLAIMS.md yet — nothing to check)")
        return 0

    errors = check(path.read_text(encoding="utf-8"))
    if errors:
        print(f"\n❌ CLAIM LEDGER FAILED — {len(errors)} violation(s):", file=sys.stderr)
        for e in errors:
            print(f"   • {e}", file=sys.stderr)
        print("\n   A DEFEND claim needs: a checked evidence box, a checked teach-back box, "
              "and citations that resolve on disk. Otherwise mark it DROP.", file=sys.stderr)
        return 1
    print("✅ claim ledger OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
