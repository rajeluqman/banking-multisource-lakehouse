#!/usr/bin/env python3
"""Secrets scan gate — config-driven, reads gates/framework.yml `secrets_scan:`.

Kit v1.1.0 addition (journey/09_SECURITY_AND_ACCESS.md). Detects the shapes named in that doc:
literal-assigned password/api_key/secret/token variables, AWS access-key IDs, private-key file
headers, and connection strings with an embedded password. Stdlib only ($0, no deps).

Two named failure classes from v1.0.0's dry-run validation (see CHANGELOG) apply here too, so
this script is deliberately conservative:
  - **hollow gate** — a check that never actually fires. Guarded against by `--self-test`, which
    plants a fake secret in an isolated temp file and asserts detection still works.
  - **self-matching regex** — a gate whose own source/docs get flagged by its own patterns.
    Guarded against by never writing a literal `name = "value"`-shaped example in this file's
    docstrings/comments (describe the shape in prose instead), plus an inline `secrets-scan:allow`
    marker for the rare doc that legitimately needs to show one.

Exit 0 = no hits. Exit 1 = at least one match (or `--self-test` found the detector broken).

Run:  python gates/secrets_scan.py
      python gates/secrets_scan.py --self-test
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

from _config import get, load_config

REPO = Path(__file__).resolve().parent.parent
ALLOW_MARKER = "secrets-scan:allow"

# Binary/non-text extensions — skipped outright rather than decoded with errors="ignore", which
# would otherwise let binary garbage generate noisy or slow regex passes.
BINARY_EXT = {
    ".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".parquet", ".zip", ".gz", ".tar",
    ".woff", ".woff2", ".ttf", ".eot", ".pyc", ".so", ".db", ".duckdb",
}

# Built-in detection patterns. Each: (label, compiled regex). The identifier is matched as a
# *substring* (not a whole-word boundary) so DB_PASSWORD / AWS_SECRET_ACCESS_KEY / my_api_key all
# still trigger, not just the bare word.
LITERAL_ASSIGN_RE = re.compile(
    r"(?i)[A-Za-z_][A-Za-z0-9_]*(?:password|passwd|api[_-]?key|secret|token)[A-Za-z0-9_]*"
    r"\s*[:=]\s*[\"']([^\"']{8,})[\"']"
)
AWS_KEY_RE = re.compile(r"AKIA[0-9A-Z]{16}")
PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----"
)
CONN_STRING_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^\s'\"/@]+:[^\s'\"/@]+@")

BUILTIN_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("literal secret-shaped assignment", LITERAL_ASSIGN_RE),
    ("AWS access key ID", AWS_KEY_RE),
    ("private key header", PRIVATE_KEY_RE),
    ("connection string with embedded password", CONN_STRING_RE),
]


def _all_patterns(config: dict) -> list[tuple[str, re.Pattern]]:
    extra = get(config, "secrets_scan.extra_patterns", []) or []
    patterns = list(BUILTIN_PATTERNS)
    for i, raw in enumerate(extra):
        patterns.append((f"extra_patterns[{i}]", re.compile(raw)))
    return patterns


def _tracked_files() -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "ls-files"], cwd=REPO, capture_output=True, text=True, check=True
        ).stdout
        return [REPO / line for line in out.splitlines() if line.strip()]
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Not a git repo (or git unavailable) — fall back to a plain walk, same skip-list
        # convention as boundary_contract.py.
        skip_dirs = {".git", "venv", "__pycache__", "node_modules", "target"}
        return [
            p for p in REPO.rglob("*")
            if p.is_file() and not any(part in skip_dirs for part in p.relative_to(REPO).parts)
        ]


def _is_allowlisted(rel: Path, allowlist: list[str]) -> bool:
    return any(rel.match(g) or str(rel) == g for g in allowlist)


def scan_file(path: Path, patterns: list[tuple[str, re.Pattern]]) -> list[str]:
    if path.suffix.lower() in BINARY_EXT or not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    errors: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if ALLOW_MARKER in line:
            continue
        for label, pattern in patterns:
            if pattern.search(line):
                errors.append(f"{path}:{lineno}: {label}")
    return errors


def check(config: dict) -> list[str]:
    allowlist = get(config, "secrets_scan.allowlist_paths", []) or [".env.example"]
    patterns = _all_patterns(config)
    errors: list[str] = []
    for f in _tracked_files():
        if not f.is_file():
            continue
        rel = f.relative_to(REPO)
        if _is_allowlisted(rel, allowlist):
            continue
        for e in scan_file(f, patterns):
            errors.append(e.replace(str(f), str(rel)))
    return errors


def self_test() -> bool:
    """Plant a fake secret in an isolated temp file; assert detection fires and a clean file
    doesn't. Proves the gate isn't hollow without ever touching the real repo tree."""
    patterns = _all_patterns({})
    with tempfile.TemporaryDirectory() as td:
        clean = Path(td) / "clean_config.py"
        clean.write_text('api_key = os.environ["API_KEY"]\nname = "creative_intel"\n')
        dirty = Path(td) / "dirty_config.py"
        # Obviously-fake, non-functional value — this is the detector's own test fixture only.
        dirty.write_text('aws_access_key_id = "AKIA' + "FAKE1234567890AB" + '"\n')

        clean_hits = scan_file(clean, patterns)
        dirty_hits = scan_file(dirty, patterns)

    ok = (not clean_hits) and bool(dirty_hits)
    print(f"self-test: clean file hits={len(clean_hits)} (want 0), "
          f"dirty file hits={len(dirty_hits)} (want >=1)")
    print("SELF-TEST PASS" if ok else "SELF-TEST FAIL — detector is hollow or over-firing")
    return ok


def main() -> int:
    if "--self-test" in sys.argv:
        return 0 if self_test() else 1

    config = load_config()
    errors = check(config)
    if errors:
        print(f"\n❌ SECRETS SCAN FAILED — {len(errors)} hit(s):", file=sys.stderr)
        for e in errors:
            print(f"   • {e}", file=sys.stderr)
        print(
            "\n   Rotate any real credential FIRST, then remove the literal. Allowlist a path in "
            "gates/framework.yml secrets_scan.allowlist_paths only for genuine non-secret "
            "examples (e.g. .env.example), or mark a single line with 'secrets-scan:allow'.",
            file=sys.stderr,
        )
        return 1
    print("✅ secrets scan OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
