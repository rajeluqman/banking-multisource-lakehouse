#!/usr/bin/env python3
"""Journey completeness gate — every mandatory journey/*.md doc must exist and be filled in.

This is the machine half of the "full-set-mandatory" owner ruling (see 00_START_HERE.md):
a doc may legitimately say "N/A — <reason>" for this project, but it may not be missing,
empty, or left as the raw unfilled template.

Deterministic check, not a content heuristic: every template ships with a sentinel line
`<!-- FRAMEWORK_TEMPLATE: UNFILLED -->` as line 1. Filling in the doc means removing that
line (00_START_HERE.md step 3 instructs this). A gate that "guesses" a doc is filled in by
scanning for leftover prose is unreliable — the first version of this script tried a
placeholder/heading heuristic and passed on completely unfilled templates in dry-run
validation, because the templates are written as real instructional prose, not `{{tokens}}`.
Sentinel-based detection can't have that false-negative.

Exit 0 = every required doc exists, and each either has no sentinel (filled in) or contains
an honest inline "N/A — <reason>" despite still carrying the sentinel. Exit 1 = a doc is
missing, empty, or still carries the sentinel with no N/A reason given.

Run:  python gates/journey_completeness.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from _config import get, load_config

REPO = Path(__file__).resolve().parent.parent
SENTINEL = "FRAMEWORK_TEMPLATE: UNFILLED"
# Real N/A markers only — excludes the boilerplate instruction line every template ships with
# ("> If not applicable: `N/A — <reason>`."), which would otherwise always self-match.
NA_RE = re.compile(r"N/A\s*—(?!\s*<reason>)", re.IGNORECASE)


def check(config: dict) -> list[str]:
    errors: list[str] = []
    required = get(config, "journey.required_docs", []) or []

    if not required:
        errors.append("gates/framework.yml journey.required_docs is empty — fill it in")
        return errors

    for doc in required:
        path = REPO / doc
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


def main() -> int:
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
