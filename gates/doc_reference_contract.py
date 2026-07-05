#!/usr/bin/env python3
"""Doc-reference contract — config-driven, reads gates/framework.yml → paths:.

Ported from creative_intelligence_lab's tests/doc_reference_contract.py, generalized: path
roots and model globs come from framework.yml instead of being hardcoded per project. Proves
every model/seed name and repo path a Markdown doc references ACTUALLY EXISTS — doc drift fails
the build instead of misleading the next reader.

Stdlib only. Exit 0 = every checked reference resolves.

What it checks:
  C1  model-shaped backtick tokens (prefix fact_/fct_/dim_/stg_/int_/bridge_/mart_/map_) must
      resolve to a real model/snapshot file (per model_globs/snapshot_globs in framework.yml).
  C2  backtick tokens and []() link targets starting with a configured path_root must exist.

What it deliberately does NOT check: prose outside backticks, external URLs, column existence.

Run:  python gates/doc_reference_contract.py [doc.md ...]     # default: journey/*.md + governance/*.md
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from _config import get, load_config

REPO = Path(__file__).resolve().parent.parent
MODEL_TOKEN = re.compile(r"^(?:fact|fct|dim|stg|int|bridge|mart|map)_[a-z0-9_]+$")


def _known_objects(config: dict) -> set[str]:
    objs: set[str] = set()
    for pattern in get(config, "paths.model_globs", []) or []:
        objs |= {p.stem for p in REPO.glob(pattern)}
    for pattern in get(config, "paths.snapshot_globs", []) or []:
        objs |= {p.stem for p in REPO.glob(pattern)}
    return objs


def _default_docs() -> list[Path]:
    docs = []
    for d in ("journey", "governance"):
        p = REPO / d
        if p.exists():
            docs += sorted(p.rglob("*.md"))
    return docs


def check(config: dict, docs: list[Path]) -> list[str]:
    known = _known_objects(config)
    path_roots = tuple(get(config, "paths.path_roots", []) or [])
    errors: list[str] = []

    backtick = re.compile(r"`([^`]+)`")
    link = re.compile(r"\]\(([^)]+)\)")

    for doc in docs:
        if not doc.exists():
            errors.append(f"{doc}: doc file does not exist")
            continue
        rel = doc.relative_to(REPO)
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
                if not path_roots or not cand.startswith(path_roots):
                    continue
                if "*" in cand or "{" in cand:
                    continue
                if not (REPO / cand).exists():
                    errors.append(f"{rel}:{lineno}  C2 path `{cand}` referenced but not found on disk")

    return errors


def main(argv: list[str]) -> int:
    config = load_config()
    docs = [Path(a) for a in argv[1:]] if len(argv) > 1 else _default_docs()
    docs = [d if d.is_absolute() else (REPO / d) for d in docs]
    errors = check(config, docs)
    if errors:
        print(f"DOC-REFERENCE CONTRACT: {len(errors)} drift violation(s)\n")
        for e in errors:
            print(f"  ✗ {e}")
        print("\nFix the doc or add a reasoned allowlist entry. Drift is a lie waiting to mislead.")
        return 1
    print(f"DOC-REFERENCE CONTRACT: OK — {len(docs)} doc(s), all references resolve.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
