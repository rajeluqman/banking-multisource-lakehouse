#!/usr/bin/env python3
"""Governance hook — config-driven, reads gates/framework.yml → governed_paths:.

Ported from creative_intelligence_lab's .claude/hooks/governance_guard.py. Wire in
.claude/settings.json for Edit|Write|MultiEdit:
  • PreToolUse  → non-blocking reminder citing the governing docs when the target path matches
                  a governed_paths entry.
  • PostToolUse → auto-runs the matching gates/*.py after the edit; exit 2 with the failure so
                  the agent is forced to see and fix it (hard block).

Note: governed_paths is a list-of-dicts in framework.yml, which needs real PyYAML to parse
correctly (the kit's minimal fallback parser only handles flat maps/lists of strings — see
gates/_config.py). If PyYAML isn't installed, this hook degrades to a no-op rather than crashing
the tool call; running `pip install pyyaml` unlocks it. Every dbt-based repo has PyYAML already.

Reads the hook JSON from stdin. Never crashes the tool call on its own bug (any internal error →
exit 0 — a broken hook must not block unrelated work).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent

# _config.py lives in gates/, not .claude/ — put THAT on the path (inserting .claude/ here never
# resolves _config, which crashed the hook at import time on every governed-path edit). Import
# defensively: a broken/missing config loader must degrade this hook to a no-op, never hard-crash
# a tool call.
sys.path.insert(0, str(REPO / "gates"))
try:
    from _config import get, load_config  # noqa: E402
except Exception:  # pragma: no cover
    def load_config() -> dict:  # type: ignore
        return {}

    def get(config: dict, dotted_path: str, default=None):  # type: ignore
        return default


def _load_rules() -> list[dict]:
    try:
        config = load_config()
    except Exception:
        return []
    rules = get(config, "governed_paths", [])
    return rules if isinstance(rules, list) and rules and isinstance(rules[0], dict) else []


def _matching_rules(path: str, rules: list[dict]) -> list[dict]:
    return [r for r in rules if r.get("match") and r["match"] in path]


def handle_pre(payload: dict, rules: list[dict]) -> int:
    tool_input = payload.get("tool_input", {})
    path = tool_input.get("file_path", "") or tool_input.get("path", "")
    matches = _matching_rules(path, rules)
    if not matches:
        return 0
    lines = ["STOP — this file is governed. Read before editing:"]
    for r in matches:
        lines.append(f"  - {r.get('cite', '(no citation set)')}")
    # additionalContext = non-blocking nudge. Do NOT emit {"decision": "block"} here — that
    # hard-blocks the edit permanently in Claude Code, making every governed file uneditable
    # (v1 of this port did exactly that). Enforcement lives in PostToolUse's gate run instead.
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "additionalContext": "\n".join(lines),
    }}))
    return 0


def handle_post(payload: dict, rules: list[dict]) -> int:
    tool_input = payload.get("tool_input", {})
    path = tool_input.get("file_path", "") or tool_input.get("path", "")
    matches = _matching_rules(path, rules)
    gates_dir = REPO / "gates"
    for r in matches:
        for gate in r.get("gates", []):
            result = subprocess.run(
                [sys.executable, str(gates_dir / gate)], capture_output=True, text=True
            )
            if result.returncode != 0:
                print(f"GATE FAILED: {gate}\n{result.stdout}\n{result.stderr}", file=sys.stderr)
                return 2
    return 0


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0
    rules = _load_rules()
    event = payload.get("hook_event_name", "")
    try:
        if event == "PreToolUse":
            return handle_pre(payload, rules)
        if event == "PostToolUse":
            return handle_post(payload, rules)
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
