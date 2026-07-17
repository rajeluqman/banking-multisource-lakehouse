#!/usr/bin/env python3
"""One-time initial-snapshot extraction for CDC sources — Teradata ONLY (ADR-007 D7.5,
R-40). Salesforce (source #4) no longer needs this module after ADR-006 Add #2: it has no
trigger/`_cdc_log` blind spot to work around — `SystemModstamp` is set on every record at
INSERT time, so `salesforce_extract.py`'s first run (no watermark yet -> no `WHERE`
predicate) naturally pulls everything `seed/salesforce/load_berka.py` just seeded, the same
way `jdbc_batch_common.py`'s first-run full-pull already does for Postgres/MSSQL.

seed/teradata/load_bank_marketing.py INSERTs the bulk seed data BEFORE calling
seed/common/cdc_ddl.py's setup_cdc() — the AFTER INSERT/UPDATE/DELETE triggers only fire on
changes made AFTER they're installed, so the seed-time bulk load never appears in
`_cdc_log`, and pipeline/extract/cdc_common.py's poll_cdc_log (which only ever reads
`_cdc_log`, never the base table) would never land it anywhere. Without this module, the
seed data silently never reaches Bronze at all for this source.

Fix: a one-time, plain full read of the just-seeded rows (same shape as
pipeline/extract/jdbc_batch_common.py's first-run full-pull — parquet + row-count manifest,
not the `_cdc_log` events shape), landed into Landing and promoted through the SAME
pipeline/promote/promotion_gate.py gate (mode="batch") as every other first-run batch pull.
Called by the seed loader itself, reusing the DataFrame already built in memory (no second
round-trip query against the source DB needed) — guarded by a `<table>_initial_snapshot`
watermark so re-running a seed script doesn't silently re-snapshot a second time.

Not wired into the Silver domain pipelines yet — silver_marketing.py currently only reads
the `_cdc` op-log Bronze table (ADR-007 D7.1; silver_crm.py no longer applies, ADR-006 Add
#2 moved it off this module entirely, see above). Landing/promoting this data is this
module's whole job (R-40's stated problem: "the seed data never reaches Bronze"); making
Silver UNION it in is a follow-on, not part of this ADR's task list — tracked honestly in
BUILD_REPORT.md rather than silently expanded into scope this module doesn't own.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pandas as pd

from pipeline.common import s3_io
from pipeline.common.lake_paths import layer_path
from pipeline.common.watermark import read_watermark, write_watermark


def extract_initial_snapshot(df: pd.DataFrame, source: str, table: str) -> str | None:
    """Lands `df` (the rows just written to `source`.`table`, all columns, verbatim) into
    Landing as one `dt=<today>` parquet partition. Returns the partition path written, or
    None if this table's initial snapshot was already taken (idempotency guard — a second
    seed-script run must not re-land the same bulk load a second time)."""
    watermark_key = f"{table}_initial_snapshot"
    if read_watermark(source, watermark_key) is not None:
        return None

    run_date = dt.date.today().isoformat()
    partition_path = layer_path("landing", source, table, f"dt={run_date}")
    _write_snapshot(partition_path, source, table, df)
    write_watermark(source, watermark_key, dt.datetime.now(dt.timezone.utc).isoformat())
    return partition_path


def _write_snapshot(partition_path: str, source: str, table: str, df: pd.DataFrame) -> None:
    """Parquet payload + manifest + `_SUCCESS` — written LAST, mirroring
    jdbc_batch_common.py's discipline so a mid-write failure leaves no `_SUCCESS` and the
    promotion gate correctly treats the partition as incomplete."""
    out_dir = Path(partition_path.replace("s3://", "/tmp/s3_staging/"))
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "part-00000.parquet", engine="pyarrow", index=False)

    manifest = {
        "source": source, "table": table, "row_count": len(df),
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    manifest_json = json.dumps(manifest)
    manifest["checksum"] = hashlib.sha256(manifest_json.encode()).hexdigest()
    (out_dir / "_manifest.json").write_text(json.dumps(manifest))
    (out_dir / "_SUCCESS").write_text("")

    # Local staging was previously the FINAL location (never pushed to S3 at all) — real AWS
    # creds should mean real S3, same fix already applied to jdbc_batch_common.py (2026-07-17).
    if s3_io.is_s3(partition_path):
        s3_io.upload_dir(str(out_dir), partition_path)
