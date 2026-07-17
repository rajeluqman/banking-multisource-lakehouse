"""Shared CDC-poll extraction logic — Teradata ONLY (ADR-006 D6.3). Salesforce (source #4)
moved off this pattern entirely in ADR-006 Add #2 (Bulk API 2.0 + `SystemModstamp`
watermark instead — see pipeline/extract/salesforce_extract.py); this module now serves
only teradata_extract.py, kept generic (the `connection`/`source` parameters) in case a
future CDC-trigger source is added, not because two callers currently share it.

Polls each table's `_cdc_log` shadow table (created at seed time by
seed/common/cdc_ddl.py) ordered by `seq`, tracks its own offset (last-processed `seq`,
stored in the lake like the batch watermark), and lands each poll's events into Landing
as a `dt=YYYY-MM-DD` partition — same shape as every other Landing arrival, so the
Landing->Bronze promotion gate treats it identically (dedup, `_SUCCESS`, ADR-003).

Deliberately NOT platform-native (no Teradata QueryGrid) — see
governance/BOUNDARY_CONTRACT.md. `connection` is any PEP 249 DB-API connection
(teradatasql qualifies).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

from pipeline.common import s3_io
from pipeline.common.lake_paths import layer_path
from pipeline.common.watermark import read_watermark, write_watermark


def poll_cdc_log(connection, source: str, table: str) -> str | None:
    """Polls `<table>_cdc_log` for events after the last-processed `seq` offset. Returns
    the Landing partition path written, or None if there were no new events (no partition
    is written for an empty poll — an empty `_SUCCESS` partition would just be noise)."""
    last_seq = read_watermark(source, f"{table}_cdc_log")
    since_seq = int(last_seq) if last_seq is not None else 0

    cur = connection.cursor()
    cur.execute(
        f"SELECT seq, op, pk_value, changed_at FROM {table}_cdc_log "
        f"WHERE seq > ? ORDER BY seq ASC",
        (since_seq,),
    )
    rows = cur.fetchall()
    if not rows:
        return None

    events = [
        {"seq": r[0], "op": r[1], "pk_value": r[2], "changed_at": str(r[3])}
        for r in rows
    ]
    max_seq = max(e["seq"] for e in events)

    run_date = dt.date.today().isoformat()
    partition_path = layer_path("landing", source, f"{table}_cdc", f"dt={run_date}")
    _write_events(partition_path, source, table, events)
    write_watermark(source, f"{table}_cdc_log", str(max_seq))
    return partition_path


def _write_events(partition_path: str, source: str, table: str, events: list[dict]) -> None:
    """Verbatim event JSON (mirrors the OBP verbatim-JSON discipline, R-19) + manifest +
    `_SUCCESS` — written LAST, after the event payload, so a mid-write failure leaves the
    partition correctly incomplete (dedup of a redelivered poll happens at the promotion
    gate via (pk_value, op, seq), R-36/R-37)."""
    out_dir = Path(partition_path.replace("s3://", "/tmp/s3_staging/"))
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(events)
    (out_dir / "events.json").write_text(payload)

    manifest = {
        "source": source, "table": table, "event_count": len(events),
        "min_seq": min(e["seq"] for e in events), "max_seq": max(e["seq"] for e in events),
        "checksum": hashlib.sha256(payload.encode()).hexdigest(),
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest))
    (out_dir / "_SUCCESS").write_text("")

    # Local staging was previously the FINAL location (never pushed to S3 at all) — real AWS
    # creds should mean real S3, same fix already applied to jdbc_batch_common.py (2026-07-17).
    if s3_io.is_s3(partition_path):
        s3_io.upload_dir(str(out_dir), partition_path)
