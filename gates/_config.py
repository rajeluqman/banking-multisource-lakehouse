"""Shared config loader for gates/*.py — reads gates/framework.yml.

Uses PyYAML if available (virtually guaranteed in any dbt-based repo); falls back to a minimal
inline parser covering the subset of YAML this file actually uses (nested maps, lists of
strings, no anchors/flow-style) so the kit still works with zero extra dependencies.
"""

from __future__ import annotations

from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "framework.yml"


def _minimal_yaml_parse(text: str) -> dict:
    root: dict = {}
    stack = [(-1, root)]
    lines = [l for l in text.splitlines() if l.strip() and not l.strip().startswith("#")]
    i = 0
    while i < len(lines):
        line = lines[i]
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]

        if stripped.startswith("- "):
            i += 1
            continue  # list items handled by lookahead below
        if ":" not in stripped:
            i += 1
            continue
        key, _, val = stripped.partition(":")
        key = key.strip()
        val = val.strip()

        if val == "" or val == "{}":
            # peek ahead: list or nested map or empty
            j = i + 1
            items: list[str] = []
            is_list = False
            while j < len(lines):
                nxt = lines[j]
                nxt_indent = len(nxt) - len(nxt.lstrip(" "))
                if nxt_indent <= indent:
                    break
                if nxt.strip().startswith("- "):
                    is_list = True
                    items.append(nxt.strip()[2:].strip().strip('"'))
                j += 1
            if is_list:
                parent[key] = items
                i = j
                continue
            else:
                new_map: dict = {}
                parent[key] = new_map
                stack.append((indent, new_map))
                i += 1
                continue
        else:
            parent[key] = val.strip('"')
            i += 1
    return root


def load_config() -> dict:
    text = CONFIG_PATH.read_text()
    try:
        import yaml  # type: ignore

        return yaml.safe_load(text) or {}
    except ImportError:
        return _minimal_yaml_parse(text)


def get(config: dict, dotted_path: str, default=None):
    node = config
    for part in dotted_path.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node
