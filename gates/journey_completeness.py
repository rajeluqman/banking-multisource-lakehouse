#!/usr/bin/env python3
"""Journey completeness gate — every mandatory journey/*.md doc must exist and be filled in.

Ported from the owner's G5 canon (banking-multisource-lakehouse/gates/journey_completeness.py),
generalized (repo root is a parameter, not a hardcoded global) and given a `--self-test` mode
per D-12/F3 (SPEC_framework-supervisor.md §6.2 A, §6.4): a gate that has never been proven to
catch what it exists to catch is vigilance wearing a gate's clothes, not enforcement. See
`kit/gates/fixtures/journey_completeness_should_fail/` for the planted violation this asserts
against.

Deterministic check, not a content heuristic: every template ships with a sentinel line
`<!-- FRAMEWORK_TEMPLATE: UNFILLED -->` as line 1. Filling in the doc means removing that line.
A gate that "guesses" a doc is filled in by scanning for leftover prose is unreliable — the
first version of this script (in the G5 lineage) tried a placeholder/heading heuristic and
passed on completely unfilled templates, because the templates are written as real
instructional prose, not `{{tokens}}`. Sentinel-based detection can't have that false-negative.

Exit 0 = every required doc exists, and each either has no sentinel (filled in) or contains
an honest inline "N/A — <reason>" despite still carrying the sentinel. Exit 1 = a doc is
missing, empty, or still carries the sentinel with no N/A reason given.

Run:  python gates/journey_completeness.py
      python gates/journey_completeness.py --self-test
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

from _config import get, load_config

REPO = Path(__file__).resolve().parent.parent
SENTINEL = "FRAMEWORK_TEMPLATE: UNFILLED"
# Real N/A markers only — excludes the boilerplate instruction line every template ships with
# ("> If not applicable: `N/A — <reason>`."), which would otherwise always self-match.
NA_RE = re.compile(r"N/A\s*—(?!\s*<reason>)", re.IGNORECASE)


def check(config: dict, repo: Path = REPO) -> list[str]:
    errors: list[str] = []
    required = get(config, "journey.required_docs", []) or []

    if not required:
        errors.append("gates/framework.yml journey.required_docs is empty — fill it in")
        return errors

    for doc in required:
        path = repo / doc
        if not path.exists():
            errors.append(f"{doc}: MISSING — required journey doc must exist (or be marked N/A inside)")
            continue
        text = path.read_text(errors="ignore")
        if not text.strip():
            errors.append(f"{doc}: EMPTY")
            continue
        if SENTINEL not in text:
            continue  # sentinel removed — treated as filled in
        body_lines = [l for l in text.splitlines() if not l.strip().startswith(">")]
        if NA_RE.search("\n".join(body_lines)):
            continue  # still carries sentinel but has an honest N/A reason outside the boilerplate — allowed
        errors.append(
            f"{doc}: still the unfilled template (sentinel present, no N/A) — "
            "fill it in and remove the FRAMEWORK_TEMPLATE sentinel line, or write 'N/A — <reason>'"
        )

    return errors


def self_test() -> bool:
    """Plant a fixture repo with one filled doc, one honestly-N/A doc, and one still-unfilled
    doc. Assert the gate passes the first two and fails only the third. Never touches the real
    repo tree — this proves the gate isn't hollow without any risk of false-clean on a real run."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "journey").mkdir()
        (root / "journey" / "01_FILLED.md").write_text("# Sources\n\nReal content, no sentinel.\n")
        (root / "journey" / "02_NA.md").write_text(
            f"<!-- {SENTINEL} -->\n# Security\n\nN/A — this project has no external access surface.\n"
        )
        (root / "journey" / "03_UNFILLED.md").write_text(
            f"<!-- {SENTINEL} -->\n# Pipeline Spec\n\n> If not applicable: `N/A — <reason>`.\n\nTemplate prose only.\n"
        )
        fake_config = {
            "journey": {
                "required_docs": ["journey/01_FILLED.md", "journey/02_NA.md", "journey/03_UNFILLED.md"]
            }
        }
        errors = check(fake_config, repo=root)

    ok = len(errors) == 1 and "03_UNFILLED.md" in errors[0]
    print(f"self-test: {len(errors)} error(s) (want exactly 1, on 03_UNFILLED.md)")
    for e in errors:
        print(f"   • {e}")
    print("SELF-TEST PASS" if ok else "SELF-TEST FAIL — gate is hollow or over-firing")
    return ok


def main() -> int:
    if "--self-test" in sys.argv:
        return 0 if self_test() else 1

    config = load_config()
    errors = check(config)
    if errors:
        print(f"\n❌ JOURNEY COMPLETENESS FAILED — {len(errors)} gap(s):", file=sys.stderr)
        for e in errors:
            print(f"   • {e}", file=sys.stderr)
        print("\n   See journey/00_START_HERE.md 'Why full-set-mandatory'.", file=sys.stderr)
        return 1
    print("✅ journey completeness OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
