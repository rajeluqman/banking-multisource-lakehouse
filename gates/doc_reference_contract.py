#!/usr/bin/env python3
"""Doc-reference contract — config-driven, reads gates/framework.yml -> paths:.

Ported from the owner's G5 canon (banking-multisource-lakehouse/gates/doc_reference_contract.py).
Proves every model/seed name and repo path a Markdown doc references ACTUALLY EXISTS — doc
drift fails the build instead of misleading the next reader.

**Extended per D-14** (SPEC_framework-supervisor.md §6.2 E, §6.4): the original only checked
paths inside the same repo. This is exactly the gap that let
`banking-multisource-lakehouse/CLAUDE.md:4` cite `framework_template/` (from
`creative_intelligence_lab`) for months — confirmed via `gh api .../commits?path=...` to have
NEVER existed at that path in that repo's history (RESEARCH_nasrul-crosscheck.md §6.2 E). A
same-repo-only gate cannot catch a cross-repo citation, and the supervisor's entire purpose is
to create cross-repo references (audit reports, sibling-repo pointers like the banking /
banking-airflow-dag pair) — so this class of error scales with the supervisor unless the gate
does too.

Cross-repo check (`C3`): a reference matching `../<name>/...` is resolved against
`paths.sibling_repos` in framework.yml.
  - If `<name>` is NOT a declared sibling at all -> **hard fail** (an undeclared cross-repo
    citation is exactly the `framework_template/` failure mode: nobody agreed this repo exists).
  - If `<name>` IS declared but not checked out at the expected relative path on THIS machine
    (e.g. in CI, which does not clone every sibling) -> **soft warning**, printed but not
    exit-1, tagged `(unverified locally)`. A human running this on a workstation with all
    siblings cloned gets the hard check; CI gets an honest "couldn't check" rather than a false
    green.

What it checks:
  C1  model-shaped backtick tokens (prefix fact_/fct_/dim_/stg_/int_/bridge_/mart_/map_) must
      resolve to a real model/snapshot file (per model_globs/snapshot_globs in framework.yml).
  C2  backtick tokens and []() link targets starting with a configured path_root must exist.
  C3  `../<name>/...` references must name a declared sibling repo (paths.sibling_repos);
      resolved against disk when that sibling is actually checked out.

What it deliberately does NOT check: prose outside backticks, external URLs, column existence.

Run:  python gates/doc_reference_contract.py [doc.md ...]     # default: journey/*.md + governance/*.md
      python gates/doc_reference_contract.py --self-test
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
MODEL_TOKEN = re.compile(r"^(?:fact|fct|dim|stg|int|bridge|mart|map)_[a-z0-9_]+$")
SIBLING_REF = re.compile(r"^\.\./([A-Za-z0-9_.\-]+)/")


def _known_objects(config: dict, repo: Path) -> set[str]:
    objs: set[str] = set()
    for pattern in get(config, "paths.model_globs", []) or []:
        objs |= {p.stem for p in repo.glob(pattern)}
    for pattern in get(config, "paths.snapshot_globs", []) or []:
        objs |= {p.stem for p in repo.glob(pattern)}
    return objs


def _default_docs(repo: Path) -> list[Path]:
    docs = []
    for d in ("journey", "governance"):
        p = repo / d
        if p.exists():
            docs += sorted(p.rglob("*.md"))
    return docs


def check(config: dict, docs: list[Path], repo: Path = REPO) -> tuple[list[str], list[str]]:
    """Returns (errors, warnings). Errors fail the gate; warnings are printed but don't."""
    known = _known_objects(config, repo)
    path_roots = tuple(get(config, "paths.path_roots", []) or [])
    siblings: list[str] = get(config, "paths.sibling_repos", []) or []
    errors: list[str] = []
    warnings: list[str] = []

    backtick = re.compile(r"`([^`]+)`")
    link = re.compile(r"\]\(([^)]+)\)")

    for doc in docs:
        if not doc.exists():
            errors.append(f"{doc}: doc file does not exist")
            continue
        rel = doc.relative_to(repo) if doc.is_relative_to(repo) else doc
        for lineno, line in enumerate(doc.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
            for tok in backtick.findall(line):
                tok = tok.strip()
                if MODEL_TOKEN.match(tok) and known and tok not in known:
                    errors.append(f"{rel}:{lineno}  C1 model `{tok}` referenced but not found (drift)")

            candidates = backtick.findall(line) + link.findall(line)
            for cand in candidates:
                cand = cand.strip().split("#", 1)[0].strip()
                if cand.startswith(("http://", "https://", "s3://", "mailto:")):
                    continue

                sib = SIBLING_REF.match(cand)
                if sib:
                    name = sib.group(1)
                    if name not in siblings:
                        errors.append(
                            f"{rel}:{lineno}  C3 undeclared sibling repo `../{name}/...` — "
                            "add it to gates/framework.yml paths.sibling_repos, or this is a "
                            "dangling reference (see doc_reference_contract.py's own docstring "
                            "for why this class of bug matters)"
                        )
                        continue
                    sibling_path = (repo / ".." / name).resolve()
                    if not sibling_path.exists():
                        warnings.append(
                            f"{rel}:{lineno}  C3 sibling `{name}` declared but not checked out "
                            "here (unverified locally) — cannot confirm the referenced path exists"
                        )
                        continue
                    target = (repo / ".." / cand).resolve()
                    if "*" in cand or "{" in cand:
                        continue
                    if not target.exists():
                        errors.append(f"{rel}:{lineno}  C3 path `{cand}` referenced but not found in sibling repo")
                    continue

                if not path_roots or not cand.startswith(path_roots):
                    continue
                if "*" in cand or "{" in cand:
                    continue
                if not (repo / cand).exists():
                    errors.append(f"{rel}:{lineno}  C2 path `{cand}` referenced but not found on disk")

    return errors, warnings


def self_test() -> bool:
    """Plant a doc with: a resolvable same-repo path (pass), a missing same-repo path (fail,
    C2), an undeclared sibling reference (fail, C3), a declared-but-absent sibling reference
    (warning only, C3), and a declared+present sibling with a bad path inside it (fail, C3)."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "main_repo"
        root.mkdir()
        (root / "journey").mkdir()
        (root / "journey" / "07_PIPELINE_SPEC.md").write_text("exists\n")

        sibling_present = Path(td) / "sibling-b"
        sibling_present.mkdir()
        (sibling_present / "README.md").write_text("hi\n")

        doc = root / "journey" / "01_TEST.md"
        doc.write_text(
            "Same-repo OK: `journey/07_PIPELINE_SPEC.md`\n"
            "Same-repo MISSING: `journey/99_GHOST.md`\n"
            "Undeclared sibling: `../framework_template/README.md`\n"
            "Declared, absent locally: `../sibling-a/README.md`\n"
            "Declared, present, bad path: `../sibling-b/NOPE.md`\n"
        )

        fake_config = {
            "paths": {
                "path_roots": ["journey/"],
                "sibling_repos": ["sibling-a", "sibling-b"],
            }
        }
        errors, warnings = check(fake_config, [doc], repo=root)

    checks = {
        "missing same-repo path (C2)": any("99_GHOST.md" in e for e in errors),
        "undeclared sibling (C3, hard fail)": any("framework_template" in e for e in errors),
        "declared absent sibling -> warning not error": (
            any("sibling-a" in w for w in warnings) and not any("sibling-a" in e for e in errors)
        ),
        "declared present sibling, bad path (C3, hard fail)": any("sibling-b" in e and "NOPE.md" in e for e in errors),
        "resolvable path did NOT fire": not any("07_PIPELINE_SPEC" in e for e in errors),
    }
    ok = all(checks.values())
    print(f"self-test: {len(errors)} error(s), {len(warnings)} warning(s)")
    for name, passed in checks.items():
        print(f"   {'✓' if passed else '✗'} {name}")
    print("SELF-TEST PASS" if ok else "SELF-TEST FAIL — gate is hollow, over-firing, or mishandling siblings")
    return ok


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return 0 if self_test() else 1

    config = load_config()
    docs = [Path(a) for a in argv[1:]] if len(argv) > 1 else _default_docs(REPO)
    docs = [d if d.is_absolute() else (REPO / d) for d in docs]
    errors, warnings = check(config, docs)

    if warnings:
        print(f"DOC-REFERENCE CONTRACT: {len(warnings)} warning(s) (not blocking)\n")
        for w in warnings:
            print(f"  ⚠ {w}")
        print()

    if errors:
        print(f"DOC-REFERENCE CONTRACT: {len(errors)} drift violation(s)\n")
        for e in errors:
            print(f"  ✗ {e}")
        print("\nFix the doc, declare the sibling in framework.yml, or the reference is a lie waiting to mislead.")
        return 1
    print(f"DOC-REFERENCE CONTRACT: OK — {len(docs)} doc(s), all references resolve.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
