#!/usr/bin/env python3
"""Stack + scope boundary contract — config-driven, reads gates/framework.yml.

Ported from creative_intelligence_lab's tests/boundary_contract.py pattern, generalized so no
project-specific values live in this file — every banned import, sanctioned override, and
locked adapter comes from framework.yml → boundary:. Edit the YAML, not this script, when
retargeting to a new project (this is the fix for the retrofit lesson: 4 repos each had a
hand-edited copy of this file with identical logic and different hardcoded values).

Stdlib only ($0, no extra deps beyond PyYAML-if-present, see _config.py). Exit 0 = holds.

Run:  python gates/boundary_contract.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from _config import get, load_config

REPO = Path(__file__).resolve().parent.parent

IMPORT_RE = re.compile(r"^\s*(?:import|from)\s+([A-Za-z0-9_.]+)")
TYPE_RE = re.compile(r"^\s*type:\s*(\S+)")


def _module_hit(module: str, deny: dict[str, str]) -> tuple[str, str] | None:
    """Return (matched deny key, reason) — the key is needed downstream for sanctioned-override
    lookup, and deriving it separately from the match is how v1 crashed (StopIteration on a
    multi-segment key like 'google.ads' hit by an exact-module import)."""
    parts = module.lower().split(".")
    for i in range(1, len(parts) + 1):
        prefix = ".".join(parts[:i])
        if prefix in deny:
            return prefix, deny[prefix]
    return None


def _is_sanctioned(rel_path: Path, module_key: str, overrides: dict[str, list[str]]) -> bool:
    globs = overrides.get(module_key, [])
    return any(rel_path.match(g) for g in globs)


def check(config: dict) -> list[str]:
    errors: list[str] = []
    banned: dict[str, str] = get(config, "boundary.banned_imports", {}) or {}
    overrides: dict[str, list[str]] = get(config, "boundary.sanctioned_overrides", {}) or {}
    profile_files: list[str] = get(config, "boundary.profile_files", []) or []
    locked_adapter = get(config, "boundary.locked_adapter", "")

    if banned:
        for path in REPO.rglob("*.py"):
            if any(part.startswith(".") or part in ("venv", "__pycache__", "node_modules")
                   for part in path.relative_to(REPO).parts):
                continue
            rel = path.relative_to(REPO)
            for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), start=1):
                m = IMPORT_RE.match(line)
                if not m:
                    continue
                module = m.group(1)
                hit = _module_hit(module, banned)
                if not hit:
                    continue
                hit_key, reason = hit
                if _is_sanctioned(rel, hit_key, overrides):
                    continue
                errors.append(f"{rel}:{lineno}: banned import '{module}' — {reason}")

    if locked_adapter:
        for name in profile_files:
            path = REPO / name
            if not path.exists():
                continue
            rel = path.relative_to(REPO)
            for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), start=1):
                m = TYPE_RE.match(line)
                if m and m.group(1) != locked_adapter:
                    errors.append(
                        f"{rel}:{lineno}: adapter type '{m.group(1)}' != '{locked_adapter}' "
                        "— see governance/BOUNDARY_CONTRACT.md"
                    )

    return errors


def main() -> int:
    config = load_config()
    errors = check(config)
    if errors:
        print(f"\n❌ BOUNDARY CONTRACT FAILED — {len(errors)} violation(s):", file=sys.stderr)
        for e in sorted(set(errors)):
            print(f"   • {e}", file=sys.stderr)
        print("\n   See governance/BOUNDARY_CONTRACT.md + gates/framework.yml. Fix before proceeding.",
              file=sys.stderr)
        return 1
    print("✅ boundary contract OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
