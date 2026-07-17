"""Locate seed-time artifacts without relying on `__file__` or an assumed CWD.

Databricks `spark_python_task` with `source: GIT` executes each file via `exec(compile(...))`
in a scope where `__file__` is undefined (`NameError`, real/reproducible: job run
127330185225331, repair 590317173511750) — a local dev-loop run has `__file__` and CWD ==
repo root, neither holds on that execution path. Search upward from CWD instead: whatever
directory the task actually starts in, it is somewhere inside the checked-out repo tree, so an
ancestor of CWD is the repo root.
"""

from __future__ import annotations

import os
from pathlib import Path


def find_seed_artifact(filename: str) -> str:
    here = Path(os.getcwd()).resolve()
    for candidate in (here, *here.parents):
        target = candidate / "seed" / "artifacts" / filename
        if target.exists():
            print(f"find_seed_artifact: cwd={here} resolved={target}")
            return str(target)
    # last-resort fallback: relative path, correct only if CWD happens to be repo root — kept
    # diagnostic (not silent) since this is the second live-debugged CWD assumption for this
    # execution path (job run 127330185225331 attempts 0 and 1) and a third surprise should be
    # fast to diagnose from run output, not re-guessed blind.
    print(f"find_seed_artifact: NO ANCESTOR OF cwd={here} CONTAINS seed/artifacts/{filename} — falling back to relative path")
    return f"seed/artifacts/{filename}"
