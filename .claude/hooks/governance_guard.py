#!/usr/bin/env python3
"""Governance hook — config-driven, reads gates/framework.yml, and emits every block to the
incident ledger.

Ported from the owner's G5 canon (banking-multisource-lakehouse/.claude/hooks/governance_guard.py)
and extended for this framework's central design idea (SPEC §8.1): enforcement events are
already the raw material for teaching, so this hook is the FIRST of the three places
(PreToolUse, PostToolUse, and — separately — a real gate failure in CI) that writes to
`learning/INCIDENTS.jsonl`. Nothing else has to remember to log an incident; the block itself
is the log entry.

Wired in .claude/settings.json for Edit|Write|MultiEdit:
  - PreToolUse  -> non-blocking reminder citing the governing docs when the target path matches
                   a governed_paths entry (unchanged from G5 — advisory only).
  - PostToolUse -> auto-runs the matching gates/*.py after the edit; on failure, appends a
                   `gate_block` incident to the ledger, then exit 2 so the agent is forced to
                   see and fix it (hard block — unchanged from G5).

Note: governed_paths is a list-of-dicts in framework.yml, which needs real PyYAML to parse
correctly (the kit's minimal fallback parser only handles flat maps/lists of strings — see
gates/_config.py). If PyYAML isn't installed, this hook degrades to a no-op rather than
crashing the tool call; running `pip install pyyaml` unlocks it.

Reads the hook JSON from stdin. Never crashes the tool call on its own bug (any internal error
-> exit 0 — a broken hook must not block unrelated work). This includes ledger-write failures:
teaching is a byproduct of enforcement, never a precondition for it — a full disk or a
permissions error writing to learning/INCIDENTS.jsonl must not itself become the reason a real
governance violation goes unblocked.
"""

from __future__ import annotations

import sys as _sys  # same reconfigure as every gate — this hook's own prints (GATE FAILED
# banner, STOP-GATE reminder) go to stdout/stderr too, and must not mojibake either.
for _stream in (_sys.stdout, _sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent

# _config.py lives in gates/, not .claude/ — put THAT on the path.
sys.path.insert(0, str(REPO / "gates"))
try:
    from _config import get, load_config  # noqa: E402
except Exception:  # pragma: no cover
    def load_config() -> dict:  # type: ignore
        return {}

    def get(config: dict, dotted_path: str, default=None):  # type: ignore
        return default

try:
    from _incident_ledger import append as ledger_append  # noqa: E402
except Exception:  # pragma: no cover
    ledger_append = None  # type: ignore


def _load_rules() -> list[dict]:
    try:
        config = load_config()
    except Exception:
        return []
    rules = get(config, "governed_paths", [])
    return rules if isinstance(rules, list) and rules and isinstance(rules[0], dict) else []


def _matching_rules(path: str, rules: list[dict]) -> list[dict]:
    return [r for r in rules if r.get("match") and r["match"] in path]


def _log_block(gate: str, artifact: str, symptom: str) -> None:
    """Best-effort — a ledger-write failure must never affect the hook's exit code."""
    if ledger_append is None:
        return
    try:
        ledger_append(REPO, kind="gate_block", gate=gate, artifact=artifact, symptom=symptom)
    except Exception:
        pass


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
    # hard-blocks the edit permanently in Claude Code, making every governed file uneditable.
    # Enforcement lives in PostToolUse's gate run instead.
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
            # encoding="utf-8" explicit, not text=True's locale-dependent default: every gate
            # reconfigures ITS OWN stdout to utf-8 (see each gate's own stdout-reconfigure
            # line), so the bytes landing in this pipe are utf-8 regardless of the host locale.
            # On a cp1252 Windows host, text=True's default decode mangled the checkmark/emoji
            # output into literal escape sequences — caught by this hook's own smoke test.
            result = subprocess.run(
                [sys.executable, str(gates_dir / gate)],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
            )
            if result.returncode != 0:
                gate_name = Path(gate).stem
                symptom = _first_violation_line(result.stdout, result.stderr)
                _log_block(gate=gate_name, artifact=path, symptom=symptom)
                print(f"GATE FAILED: {gate}\n{result.stdout}\n{result.stderr}", file=sys.stderr)
                return 2
    return 0


def _first_violation_line(stdout: str, stderr: str) -> str:
    """Every gate's failure output ends with a fixed 'see docs / fix this' hint line — that is
    the WRONG thing to log as the incident symptom (it's boilerplate, identical across every
    failure of that gate). The actual violation is the first bulleted `   • ...` detail line
    right after the '❌ ... FAILED' header. Falls back to the first non-empty line of whichever
    stream has content if no bullet is found, rather than crashing on an unexpected gate output
    shape."""
    combined = stdout.strip() or stderr.strip()
    if not combined:
        return "gate failed (no output captured)"
    for line in combined.splitlines():
        stripped = line.strip()
        if stripped.startswith(("•", "-", "*")) and len(stripped) > 2:
            return stripped.lstrip("•-* ").strip()
    return next((l.strip() for l in combined.splitlines() if l.strip()), "gate failed")


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
