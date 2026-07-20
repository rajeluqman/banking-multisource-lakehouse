#!/usr/bin/env python3
"""Salesforce ("Internal CRM") -> Landing, Bulk API 2.0 + `SystemModstamp` high-watermark
(ADR-006 Add #2 — replaces `sap_hana_extract.py`'s CDC-poll pattern for source #4 only).

Same *class* of skill as `jdbc_batch_common.py`'s watermarked incremental pull (Postgres/
MSSQL), applied to a REST/Bulk SaaS surface instead of JDBC: first run pulls everything
(no `WHERE SystemModstamp` predicate), every subsequent run pulls only rows changed since
the last watermark (minus `OVERLAP_WINDOW`, R-26, same discipline as the JDBC extractors).
Bulk API 2.0 job lifecycle (create query job -> poll to completion -> page through CSV
results) is handled by `simple_salesforce.bulk2`; this module just shapes the SOQL, the
per-object field list, and the Landing partition (parquet + manifest + `_SUCCESS`, same
shape as `cdc_initial_snapshot.py`/`jdbc_batch_common.py` so the Landing->Bronze promotion
gate treats it as an ordinary "batch" partition, not a "cdc" one — Salesforce has no
`_cdc_log`, ADR-006 Add #2 D6.3 supersession).

Object coverage (Berka -> Salesforce, seeded by seed/salesforce/load_berka.py):
  client -> Contact, account -> Account, disp -> AccountContactRelation (native N:N),
  trans -> Transaction__c (NEW custom object), district -> District__c (NEW custom
  object), CRM tickets -> Case (synthetic, seed-time-generated — Berka has no native
  ticket table). Card/loan are NOT carried into Salesforce (ADR-006 Add #2 build-time
  scope note) — neither is read by any Gold builder, so this is a disclosed narrowing,
  not a silent data loss (BUILD_REPORT.md).

Run against the owner's provisioned Salesforce org, NOT executed in this planning
session (no live connection here — see journey/07_PIPELINE_SPEC.md prerequisites).

Run:  python pipeline/extract/salesforce_extract.py [--full-backfill]
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json

import pandas as pd

from pipeline.common import s3_io
from pipeline.common.lake_paths import layer_path
from pipeline.common.run_interval import interval_window, logical_date
from pipeline.common.watermark import read_watermark, write_watermark
from pipeline.extract.salesforce_auth import get_salesforce_client

OVERLAP_WINDOW = dt.timedelta(minutes=5)  # R-26 — same overlap discipline as jdbc_batch_common.py

# bronze_table -> (Salesforce object API name, SOQL field list, extra WHERE clause or None)
# `account` needs the extra filter because Account is a STANDARD object that ships with
# Developer Edition sample/demo records (Edge Communications, Burlington Textiles, ...,
# live-confirmed 8 of them in this org) — pulling those in gives Bronze rows with a NULL
# berka_account_id__c, and since NULL is not a distinct MERGE key, sil_account's upsert
# fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE. The other 5 objects
# are either custom (District__c/Transaction__c, seed-only, no pre-existing rows possible)
# or came back with zero pre-existing junk when live-checked (Contact/Case), so they don't
# need the same guard.
TABLES: dict[str, tuple[str, list[str], str | None]] = {
    "contact": ("Contact", ["Id", "berka_client_id__c", "birth_number__c", "berka_district_id__c", "SystemModstamp"], None),
    "account": ("Account", ["Id", "berka_account_id__c", "berka_district_id__c", "berka_frequency__c", "berka_account_open_date__c", "SystemModstamp"], "berka_account_id__c != null"),
    "accountcontactrelation": ("AccountContactRelation", ["Id", "AccountId", "ContactId", "Roles", "berka_disp_id__c", "berka_disp_type__c", "SystemModstamp"], None),
    "transaction": ("Transaction__c", ["Id", "berka_trans_id__c", "berka_account_id__c", "trans_date__c", "trans_type__c", "operation__c", "amount__c", "balance__c", "k_symbol__c", "bank__c", "partner_account__c", "SystemModstamp"], None),
    "district": ("District__c", ["Id", "berka_district_id__c", "district_name__c", "region__c", "SystemModstamp"], None),
    "case": ("Case", ["Id", "ContactId", "CreatedDate", "Type", "SystemModstamp"], None),
}


def _soql_datetime(ts: dt.datetime) -> str:
    """Salesforce SOQL datetime literal — unquoted ISO-8601 with an explicit offset."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000%z")


def _pull_records(sf, sf_object: str, fields: list[str], since: dt.datetime | None,
                   extra_where: str | None = None, until: dt.datetime | None = None) -> list[dict]:
    soql = f"SELECT {', '.join(fields)} FROM {sf_object}"
    conditions = []
    if extra_where is not None:
        conditions.append(extra_where)
    if since is not None:
        conditions.append(f"SystemModstamp > {_soql_datetime(since)}")
    if until is not None:
        conditions.append(f"SystemModstamp < {_soql_datetime(until)}")
    if conditions:
        soql += " WHERE " + " AND ".join(conditions)

    bulk_type = getattr(sf.bulk2, sf_object)
    rows: list[dict] = []
    for page_csv in bulk_type.query(soql):
        if not page_csv:
            continue
        rows.extend(csv.DictReader(io.StringIO(page_csv)))
    return rows


def extract_object(sf, source: str, bronze_table: str, full_backfill: bool = False) -> str | None:
    """Pulls `bronze_table`'s Salesforce object, lands it as parquet under Landing
    `dt=<logical date>`, writes a manifest + `_SUCCESS`, advances the watermark. Returns the
    Landing partition path, or None if the pull returned zero rows (no partition written
    for an empty pull, same discipline as `cdc_common.py.poll_cdc_log`).

    Same predicate precedence as `jdbc_batch_common.extract_table` (staff-DE ruling,
    contract-compliance fix, 2026-07-20) — full_backfill unbounded; Airflow-interval mode
    bounded `[start-overlap, end)` with no watermark read/write; else lake-watermark mode.
    """
    sf_object, fields, extra_where = TABLES[bronze_table]

    window = None if full_backfill else interval_window(OVERLAP_WINDOW)
    since: dt.datetime | None
    until: dt.datetime | None
    if full_backfill:
        since = until = None
    elif window is not None:
        since, until = window
    else:
        last_watermark = read_watermark(source, bronze_table)
        since = dt.datetime.fromisoformat(last_watermark) - OVERLAP_WINDOW if last_watermark is not None else None
        until = None

    records = _pull_records(sf, sf_object, fields, since, extra_where, until=until)
    if not records:
        return None

    df = pd.DataFrame.from_records(records)
    run_date = logical_date()
    partition_path = layer_path("landing", source, bronze_table, f"dt={run_date}")
    _write_partition(partition_path, source, bronze_table, df)

    if window is None:
        # Only advance the lake watermark in fallback mode — an interval-mode run must not
        # mutate state a later watermark-mode run depends on (see jdbc_batch_common.py).
        write_watermark(source, bronze_table, dt.datetime.now(dt.timezone.utc).isoformat())
    return partition_path


def _write_partition(partition_path: str, source: str, table: str, df: pd.DataFrame) -> None:
    """Parquet payload + manifest + `_SUCCESS` — written LAST, mirroring
    `cdc_initial_snapshot.py`'s discipline so a mid-write failure leaves no `_SUCCESS` and
    the promotion gate correctly treats the partition as incomplete. Real S3 when
    `partition_path` resolves to `s3://` (real AWS creds present, `pipeline.common.s3_io`),
    local-disk fallback otherwise — same dual-mode `pipeline.common.lake_paths` already
    promises (2026-07-16 fix: this previously always staged locally regardless of creds)."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3_io.write_bytes(f"{partition_path}/part-00000.parquet", buf.getvalue())

    manifest = {
        "source": source, "table": table, "row_count": len(df),
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    manifest_json = json.dumps(manifest)
    manifest["checksum"] = hashlib.sha256(manifest_json.encode()).hexdigest()
    s3_io.write_text(f"{partition_path}/_manifest.json", json.dumps(manifest))
    s3_io.write_text(f"{partition_path}/_SUCCESS", "")


def main(full_backfill: bool = False) -> int:
    """`full_backfill` defaults to False so this conforms to the zero-arg `main() -> int`
    contract pipeline/orchestrate.py relies on — same convention as postgres_extract.py."""
    sf = get_salesforce_client()
    for bronze_table in TABLES:
        path = extract_object(sf, "salesforce", bronze_table, full_backfill=full_backfill)
        print(f"salesforce.{bronze_table} -> {path or '(no new/changed rows)'}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-backfill", action="store_true",
                     help="force a full re-pull of every object regardless of watermark state (ADR-007 D7.4 Strategy 1)")
    args = ap.parse_args()
    _rc = main(full_backfill=args.full_backfill)
    if _rc != 0:
        raise SystemExit(_rc)
