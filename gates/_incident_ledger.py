"""Incident ledger — the join between enforcement and teaching (SPEC §8.1, §8.4).

A gate failure, a hook block, a TWO-STRIKE trigger, a diagnosed root cause: every one of these
is already an event the enforcement layer sees. Logging it here means the teaching layer
(`/de-teach`) gets its curriculum for free, built from the operator's own real incidents,
instead of two parallel tracks that have to be manually kept in sync — which is exactly how
this framework's own two `cikgu` implementations (vault skill vs repo agent) drifted apart
before this design existed.

File: `learning/INCIDENTS.jsonl` in the CONSUMING repo (not this kit repo). Append-only, one
JSON object per line. Status may only move forward:

    open -> diagnosed -> fixed -> taught -> hardened

Never edit or delete a line. `incident_hygiene.py` (the gate) enforces the forward-only and
evidence-required rules; this module only provides the read/write mechanics both the hook and
the gates share, so there is exactly one JSONL-shaped bug to fix, not several.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

LEDGER_RELATIVE_PATH = "learning/INCIDENTS.jsonl"

VALID_KINDS = {"gate_block", "hook_block", "two_strike", "ci_fail", "rca", "data_defect", "perf"}
STATUS_ORDER = ["open", "diagnosed", "fixed", "taught", "hardened"]
REQUIRED_FIELDS = {"id", "ts", "kind", "status"}


def ledger_path(repo: Path) -> Path:
    return repo / LEDGER_RELATIVE_PATH


def _next_id(repo: Path) -> str:
    existing = read_all(repo)
    nums = [int(e["id"].split("-")[1]) for e in existing if e.get("id", "").startswith("INC-")]
    n = (max(nums) + 1) if nums else 1
    return f"INC-{n:04d}"


def append(
    repo: Path,
    *,
    kind: str,
    symptom: str,
    gate: str | None = None,
    artifact: str | None = None,
    extra: dict | None = None,
) -> dict:
    """Append a new `open` incident. Returns the written record. Never overwrites — a fresh
    entry per event, even if the same gate fires repeatedly (repetition is itself the TWO-STRIKE
    signal, see incident_hygiene.py)."""
    if kind not in VALID_KINDS:
        raise ValueError(f"unknown incident kind '{kind}' — must be one of {sorted(VALID_KINDS)}")

    record = {
        "id": _next_id(repo),
        "ts": datetime.now(timezone.utc).isoformat(),
        "kind": kind,
        "gate": gate,
        "repo": repo.name,
        "artifact": artifact,
        "symptom": symptom,
        "root_cause": None,
        "fix": None,
        "lesson": None,
        "status": "open",
    }
    if extra:
        record.update(extra)

    path = ledger_path(repo)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def read_all(repo: Path) -> list[dict]:
    path = ledger_path(repo)
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records


def advance(repo: Path, incident_id: str, new_status: str, **fields) -> dict:
    """Rewrites the WHOLE ledger with one record's status/fields updated — this is the one
    sanctioned exception to 'append-only' at the file-content level, because JSONL has no
    in-place single-record update. What's actually immutable is the forward-only status
    invariant, enforced here, not the byte layout of the file. `incident_hygiene.py` re-checks
    this invariant independently in CI, so a hand-edited ledger that skips a status or moves
    backward still gets caught even if this function was bypassed."""
    if new_status not in STATUS_ORDER:
        raise ValueError(f"unknown status '{new_status}' — must be one of {STATUS_ORDER}")

    records = read_all(repo)
    updated = None
    for r in records:
        if r["id"] == incident_id:
            old_idx = STATUS_ORDER.index(r["status"])
            new_idx = STATUS_ORDER.index(new_status)
            if new_idx <= old_idx:
                raise ValueError(
                    f"{incident_id}: cannot move status '{r['status']}' -> '{new_status}' "
                    f"(forward-only: {' -> '.join(STATUS_ORDER)})"
                )
            r["status"] = new_status
            r.update(fields)
            updated = r
    if updated is None:
        raise KeyError(f"{incident_id} not found in {ledger_path(repo)}")

    path = ledger_path(repo)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return updated
