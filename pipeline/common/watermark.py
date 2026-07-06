"""Watermark / CDC-offset state, stored IN THE LAKE (journey/07_PIPELINE_SPEC.md), not in
code or a local file — so state survives a wiped-and-rebuilt Databricks cluster (ADR-002's
"compute/storage fully decoupled" point).

One small JSON file per (source, table) under `_control/watermarks/`. Deliberately not a
database — this is tiny, infrequently-written state; a JSON blob per key is simpler than
standing up a control-plane table for it, and is trivial to inspect/repair by hand if a
watermark ever needs a manual fix.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pipeline.common.lake_paths import control_path

try:
    import boto3  # only needed when reading/writing to real S3
except ImportError:  # pragma: no cover - local-disk fallback doesn't need boto3
    boto3 = None

from pathlib import Path
from urllib.parse import urlparse


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _read_text(path: str) -> str | None:
    if _is_s3(path):
        parsed = urlparse(path)
        client = boto3.client("s3")
        try:
            obj = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
            return obj["Body"].read().decode("utf-8")
        except client.exceptions.NoSuchKey:
            return None
    p = Path(path)
    return p.read_text() if p.exists() else None


def _write_text(path: str, text: str) -> None:
    if _is_s3(path):
        parsed = urlparse(path)
        client = boto3.client("s3")
        client.put_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Body=text.encode("utf-8"))
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def _watermark_path(source: str, table: str) -> str:
    return control_path("watermarks", f"{source}.{table}.json")


def read_watermark(source: str, table: str) -> str | None:
    """Returns the last-processed watermark value (an ISO timestamp for batch sources, or a
    `_cdc_log.seq` integer-as-string for CDC sources), or None if this is the first run."""
    text = _read_text(_watermark_path(source, table))
    if text is None:
        return None
    return json.loads(text)["watermark"]


def write_watermark(source: str, table: str, value: str) -> None:
    payload = {
        "source": source, "table": table, "watermark": value,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_text(_watermark_path(source, table), json.dumps(payload))
