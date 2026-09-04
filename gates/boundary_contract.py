#!/usr/bin/env python3
"""Stack + scope boundary contract — config-driven, reads gates/framework.yml.

Ported from the owner's G5 canon (banking-multisource-lakehouse/gates/boundary_contract.py),
which itself generalized creative_intelligence_lab's tests/boundary_contract.py so no
project-specific values live in this file — every banned import, sanctioned override, and
locked adapter comes from framework.yml → boundary:. Edit the YAML, not this script, when
retargeting to a new project (this is the fix for the retrofit lesson: 4 repos each had a
hand-edited copy of this file with identical logic and different hardcoded values).

`--self-test` added per D-12/F3 (SPEC §6.2 A, §6.4) — plants a fixture with a banned import,
a sanctioned override of that same import, and a clean file, asserting the gate catches
exactly the unsanctioned one.

Stdlib only ($0, no extra deps beyond PyYAML-if-present, see _config.py). Exit 0 = holds.

Run:  python gates/boundary_contract.py
      python gates/boundary_contract.py --self-test
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


def check(config: dict, repo: Path = REPO) -> list[str]:
    errors: list[str] = []
    banned: dict[str, str] = get(config, "boundary.banned_imports", {}) or {}
    overrides: dict[str, list[str]] = get(config, "boundary.sanctioned_overrides", {}) or {}
    profile_files: list[str] = get(config, "boundary.profile_files", []) or []
    locked_adapter = get(config, "boundary.locked_adapter", "")

    if banned:
        for path in repo.rglob("*.py"):
            if any(part.startswith(".") or part in ("venv", "__pycache__", "node_modules")
                   for part in path.relative_to(repo).parts):
                continue
            rel = path.relative_to(repo)
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

    for gname in ("entrypoint_guard", "no_inrepo_scheduler"):
        guard = get(config, f"boundary.{gname}", {}) or {}
        if not guard:
            continue
        banned_re = re.compile(guard.get("banned_regex", ""))
        reason = guard.get("reason", "")
        for glob in guard.get("scan_globs", []):
            for path in repo.glob(glob):
                if any(part in ("__pycache__",) for part in path.relative_to(repo).parts):
                    continue
                rel = path.relative_to(repo)
                for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), start=1):
                    if banned_re.search(line):
                        errors.append(f"{rel}:{lineno}: {gname} violation — {reason}")

    if locked_adapter:
        for name in profile_files:
            path = repo / name
            if not path.exists():
                continue
            rel = path.relative_to(repo)
            for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), start=1):
                m = TYPE_RE.match(line)
                if m and m.group(1) != locked_adapter:
                    errors.append(
                        f"{rel}:{lineno}: adapter type '{m.group(1)}' != '{locked_adapter}' "
                        "— see governance/BOUNDARY_CONTRACT.md"
                    )

    return errors


def self_test() -> bool:
    """Plant: one file with an unsanctioned banned import (must fail), one file with the SAME
    banned import but under a sanctioned override glob (must pass), one clean file (must pass).
    Also plants an entrypoint_guard violation. Asserts the gate catches exactly the two real
    violations and nothing else."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "pipeline").mkdir()
        (root / "pipeline" / "bad.py").write_text("import sklearn\n")
        (root / "pipeline" / "allowed_ml_experiment.py").write_text("import sklearn\n")
        (root / "pipeline" / "clean.py").write_text("import pandas\n")
        (root / "pipeline" / "entry.py").write_text(
            "def main():\n    return 0\n\nif __name__ == '__main__':\n    raise SystemExit(main())\n"
        )
        fake_config = {
            "boundary": {
                "banned_imports": {"sklearn": "no ML training in this repo (test fixture)"},
                "sanctioned_overrides": {"sklearn": ["pipeline/allowed_ml_experiment.py"]},
                "entrypoint_guard": {
                    "scan_globs": ["pipeline/**/*.py"],
                    "banned_regex": r"raise SystemExit\(main\(",
                    "reason": "Databricks SystemExit(0)-is-failure contract (test fixture)",
                },
            }
        }
        errors = check(fake_config, repo=root)

    hit_bad = any("bad.py" in e and "sklearn" in e for e in errors)
    hit_allowed = any("allowed_ml_experiment.py" in e for e in errors)
    hit_entry = any("entry.py" in e and "entrypoint_guard" in e for e in errors)
    hit_clean = any("clean.py" in e for e in errors)

    ok = hit_bad and hit_entry and not hit_allowed and not hit_clean and len(errors) == 2
    print(f"self-test: {len(errors)} error(s) (want exactly 2: bad.py + entry.py)")
    for e in errors:
        print(f"   • {e}")
    print("SELF-TEST PASS" if ok else "SELF-TEST FAIL — gate is hollow, over-firing, or ignoring sanctioned overrides")
    return ok


def main() -> int:
    if "--self-test" in sys.argv:
        return 0 if self_test() else 1

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
