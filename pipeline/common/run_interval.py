"""Logical-date/interval resolution — makes ADR-011 D11.5 real (PIPELINE_SIDE_CONTRACT.md §3:
"the ingest task's `data_interval_start` maps to `dt`... never `now()`").

The external Airflow orchestration repo's `dag_ingest_*` tasks set `DATA_INTERVAL_START` /
`DATA_INTERVAL_END` (ISO-8601) in the extraction executor's environment for every run,
including backfills. When absent (local dev-loop / direct-CLI runs with no Airflow above
them), everything here falls back to wall-clock `today()` — the pre-existing dev-loop
behavior, unchanged.
"""

from __future__ import annotations

import datetime as dt
import os


def logical_date() -> str:
    """The `dt=` partition key: `DATA_INTERVAL_START`'s date if Airflow set it, else today."""
    start = os.environ.get("DATA_INTERVAL_START")
    if start is not None:
        return dt.datetime.fromisoformat(start).date().isoformat()
    return dt.date.today().isoformat()


def interval_window(overlap: dt.timedelta) -> tuple[dt.datetime, dt.datetime] | None:
    """Returns `(DATA_INTERVAL_START - overlap, DATA_INTERVAL_END)` if Airflow set both env
    vars, else None (no bounded interval — caller falls back to lake-watermark mode)."""
    start = os.environ.get("DATA_INTERVAL_START")
    end = os.environ.get("DATA_INTERVAL_END")
    if start is None or end is None:
        return None
    return dt.datetime.fromisoformat(start) - overlap, dt.datetime.fromisoformat(end)
