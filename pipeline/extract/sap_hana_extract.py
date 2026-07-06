#!/usr/bin/env python3
"""SAP HANA Cloud ("Internal CRM") -> Landing, CDC-poll (ADR-006 D6.3). Thin driver — poll
logic lives in cdc_common.py, shared with teradata_extract.py.

Run against the owner's provisioned SAP HANA Cloud instance, NOT executed in this planning
session (no live connection here — see journey/07_PIPELINE_SPEC.md prerequisites).
"""

from __future__ import annotations

import os

from hdbcli import dbapi

from pipeline.extract.cdc_common import poll_cdc_log

TABLES = ["client", "account", "disp", "card", "loan", "trans", "district"]


def _connection():
    return dbapi.connect(
        address=os.environ["SAP_HANA_HOST"], port=int(os.environ.get("SAP_HANA_PORT", 443)),
        user=os.environ["SAP_HANA_USER"], password=os.environ["SAP_HANA_PASSWORD"], encrypt=True,
    )


def main() -> int:
    conn = _connection()
    for table in TABLES:
        path = poll_cdc_log(conn, "sap_hana", table)
        print(f"sap_hana.{table}_cdc -> {path or '(no new events)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
