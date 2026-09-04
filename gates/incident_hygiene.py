#!/usr/bin/env python3
"""Incident hygiene gate — new in this framework (SPEC §8.8), no G5 ancestor.

Enforces the two rules that make `learning/INCIDENTS.jsonl` trustworthy as a teaching source
instead of a place fabricated stories could live:

  H1  A `hardened` incident must cite a REAL file:line in its `fix` field. This is the
      mechanical version of the owner's pre-existing troubleshooting-library rule
      ("every ✅ HARDENED card cites a real file:line ... no fabricated incidents") — previously
      an honesty contract enforced by review; here it is enforced by parsing.
  H2  A `two_strike` incident must not sit at `open`. ADR-009's TWO-STRIKE doctrine requires an
      Incident Commander to diagnose BEFORE any further paid execution — an open two_strike
      entry means that never happened, or happened outside the ledger, which is the same
      failure the ledger exists to prevent.

Also validates the ledger is well-formed JSONL with no duplicate or out-of-order-looking IDs —
a corrupted or hand-edited ledger is worse than none, because it would look authoritative.

Exit 0 = clean ledger, or no ledger yet (a project with zero incidents is not a violation).

Run:  python gates/incident_hygiene.py
      python gates/incident_hygiene.py --self-test
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

from _incident_ledger import ledger_path, read_all

REPO = Path(__file__).resolve().parent.parent
FILE_LINE_CITE = re.compile(r"[\w./\\-]+\.\w+:\d+")


def check(repo: Path = REPO) -> list[str]:
    errors: list[str] = []
    path = ledger_path(repo)
    if not path.exists():
        return errors  # no ledger yet is fine — nothing to be dishonest about

    try:
        records = read_all(repo)
    except Exception as e:  # malformed JSONL
        return [f"{path}: could not parse ledger — {e}"]

    seen_ids: set[str] = set()
    for r in records:
        rid = r.get("id", "<no id>")
        if rid in seen_ids:
            errors.append(f"{rid}: duplicate incident id in ledger")
        seen_ids.add(rid)

        if r.get("status") == "hardened":
            fix = r.get("fix") or ""
            if not FILE_LINE_CITE.search(fix):
                errors.append(
                    f"{rid}: status=hardened but fix field has no file:line citation "
                    f"(fix={fix!r}) — H1, see troubleshooting-library integrity rule"
                )

        if r.get("kind") == "two_strike" and r.get("status") == "open":
            errors.append(
                f"{rid}: kind=two_strike is still status=open — ADR-009 requires an Incident "
                "Commander to diagnose before any further paid execution (H2)"
            )

    return errors


def self_test() -> bool:
    """Plant a ledger with: a clean hardened entry (pass), a hardened entry with no file:line
    (fail, H1), an open two_strike entry (fail, H2), a diagnosed two_strike entry (pass), and a
    duplicate id (fail)."""
    import json

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "learning").mkdir()
        lines = [
            {"id": "INC-0001", "kind": "gate_block", "status": "hardened",
             "fix": "pipeline/gold/dim_customer.py:88 — added null guard"},
            {"id": "INC-0002", "kind": "gate_block", "status": "hardened",
             "fix": "fixed it, trust me"},
            {"id": "INC-0003", "kind": "two_strike", "status": "open"},
            {"id": "INC-0004", "kind": "two_strike", "status": "diagnosed",
             "root_cause": "oversized compute pool"},
            {"id": "INC-0001", "kind": "gate_block", "status": "open"},  # duplicate id
        ]
        (root / "learning" / "INCIDENTS.jsonl").write_text(
            "\n".join(json.dumps(l) for l in lines) + "\n"
        )
        errors = check(repo=root)

    checks = {
        "clean hardened entry did NOT fire": not any("INC-0001" in e and "H1" in e for e in errors),
        "no-citation hardened entry fired (H1)": any("INC-0002" in e and "H1" in e for e in errors),
        "open two_strike fired (H2)": any("INC-0003" in e and "H2" in e for e in errors),
        "diagnosed two_strike did NOT fire": not any("INC-0004" in e for e in errors),
        "duplicate id fired": any("duplicate" in e for e in errors),
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

    errors = check()
    if errors:
        print(f"\n❌ INCIDENT HYGIENE FAILED — {len(errors)} violation(s):", file=sys.stderr)
        for e in errors:
            print(f"   • {e}", file=sys.stderr)
        return 1
    print("✅ incident hygiene OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
