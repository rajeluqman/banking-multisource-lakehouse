#!/usr/bin/env python3
"""Teradata ("Marketing / Campaign") -> Landing, CDC-poll (ADR-006 D6.3). Thin driver —
poll logic lives in cdc_common.py (Teradata-only since ADR-006 Add #2 moved Salesforce off
the CDC-poll pattern entirely).

Run against the owner's provisioned Teradata instance, NOT executed in this planning
session (no live connection here — see journey/07_PIPELINE_SPEC.md prerequisites).
"""

from __future__ import annotations

import os

import teradatasql

from pipeline.extract.cdc_common import poll_cdc_log

TABLES = ["bank_marketing"]


def _connection():
    return teradatasql.connect(
        host=os.environ["TERADATA_HOST"], user=os.environ["TERADATA_USER"],
        password=os.environ["TERADATA_PASSWORD"],
    )


def main() -> int:
    conn = _connection()
    for table in TABLES:
        path = poll_cdc_log(conn, "teradata", table)
        print(f"teradata.{table}_cdc -> {path or '(no new events)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
