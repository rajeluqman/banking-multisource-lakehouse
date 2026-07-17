#!/usr/bin/env python3
"""The Landing -> Bronze promotion gate — TRANSPORT INTEGRITY ONLY (ADR-003/D-15).

This gate judges: did we receive it completely and exactly once? It does NOT do content
cleansing (orphan FKs, nulls, birth_number decode, etc — that's the Bronze -> Silver gate,
journey/06_DQ_PLAN.md). Conflating the two is exactly what ADR-003 forbids.

Checks (in order — first failure quarantines, no partial promotion):
  1. `_SUCCESS` marker present.
  2. Manifest checksum matches the actual landed payload (catches a truncated/corrupted write).
  3. Pagination reconciled vs API-reported total, where applicable (R-22 — manifest['reconciled']).
  4. Schema-drift check against the last-known-good Bronze schema (R-16/R-28) — mismatch
     quarantines with an alert; controlled `mergeSchema` only after explicit review, never silent.
  5. Multi-file set completeness, where applicable (a generic capability — no current source
     is a multi-file drop after ADR-006 moved Berka off file-drop, first onto SAP HANA CDC
     then onto Salesforce Bulk API (Add #2), but the check stays available rather than
     removed, since D-15's contract describes it generically).
  6. Dedup: batch partitions dedup at the Silver MERGE (journey/07); CDC partitions dedup
     HERE via anti-join on (source, table, pk_value, op, seq) against what's already in
     Bronze, since a redelivered CDC poll must not double-append events (R-36/R-37).

Pass -> append to Bronze (Delta, verbatim, D-05). Fail -> quarantine in place in Landing,
alert (pipeline.common.alerts), Bronze untouched, pipeline continues on last-good Bronze.
"""

from __future__ import annotations

import hashlib
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from pipeline.common import s3_io
from pipeline.common.alerts import notify_slack_failure
from pipeline.common.lake_paths import layer_path


class QuarantineReason(Exception):
    """Raised internally when a check fails — caught by promote_partition, never lets a
    partially-checked partition through."""


def _check_success_marker(partition_path: str) -> None:
    if not s3_io.exists(f"{partition_path}/_SUCCESS"):
        raise QuarantineReason("_SUCCESS marker missing — partial/incomplete arrival")


def _load_manifest(partition_path: str) -> dict:
    text = s3_io.read_text(f"{partition_path}/_manifest.json")
    if text is None:
        raise QuarantineReason("_manifest.json missing — cannot verify transport integrity")
    return json.loads(text)


def _check_checksum(partition_path: str, manifest: dict) -> None:
    payload_file = next(
        (f for f in ("response.json", "events.json") if s3_io.exists(f"{partition_path}/{f}")), None
    )
    if payload_file is None:
        return  # JDBC batch partitions (parquet) are checked via row_count, not a payload checksum
    actual = hashlib.sha256(s3_io.read_bytes(f"{partition_path}/{payload_file}")).hexdigest()
    if actual != manifest.get("checksum"):
        raise QuarantineReason(f"checksum mismatch: manifest={manifest.get('checksum')} actual={actual}")


def _check_pagination_reconciled(manifest: dict) -> None:
    if "reconciled" in manifest and not manifest["reconciled"]:
        raise QuarantineReason(
            f"pagination not reconciled: {manifest.get('rows_landed')} landed vs "
            f"{manifest.get('api_reported_total')} API-reported (R-22)"
        )


def _check_schema_drift(spark: SparkSession, source: str, table: str, new_df) -> None:
    bronze_path = layer_path("bronze", source, table)
    if not s3_io.prefix_has_objects(bronze_path):
        return  # first-ever promotion for this table — no prior schema to drift from
    existing_schema = spark.read.format("delta").load(bronze_path).schema
    if set(f.name for f in new_df.schema.fields) != set(f.name for f in existing_schema.fields):
        raise QuarantineReason(
            "schema drift detected (R-16/R-28) — column set differs from last-known-good Bronze "
            "schema; requires explicit review + a deliberate mergeSchema, never silent auto-evolution"
        )


def promote_partition(spark: SparkSession, source: str, table: str, partition_path: str, mode: str) -> bool:
    """mode: 'batch' (JDBC/API pull, already-deduped-at-Silver) or 'cdc' (dedup here on
    (pk_value, op, seq), R-36/R-37). Returns True if promoted, False if quarantined.

    Reads Landing directly from `partition_path` via Spark's native reader (S3A) regardless of
    shape (parquet / verbatim JSON) — this stage only ever runs on the Databricks cluster
    (Bronze Delta writes need S3A, which local Spark doesn't have), so there is never a reason
    to read from local `/tmp/s3_staging/` here; that path belongs solely to the LANDING-write
    side's local-staging-then-upload trick (`s3_io.upload_dir`), not promotion. Previously 'cdc'
    (Teradata) and the `response.json` batch shape (OBP) hardcoded a local-staging read instead
    — harmless while `cdc_common.py`/`obp_client.py` never uploaded anything to S3 at all, but a
    real latent bug once they did (2026-07-17 fix): Landing lands on Databricks-unreachable
    local disk in this Codespace, promotion runs on a different machine entirely."""
    try:
        _check_success_marker(partition_path)
        manifest = _load_manifest(partition_path)
        _check_checksum(partition_path, manifest)
        _check_pagination_reconciled(manifest)

        if mode == "cdc":
            events_df = spark.read.json(f"{partition_path}/events.json")
            events_df = events_df.withColumn("source", lit(source)).withColumn("table", lit(table))
            _check_schema_drift(spark, source, f"{table}_cdc", events_df)
            _promote_cdc(spark, source, table, events_df)
        else:
            # "batch" covers two different Landing shapes: JDBC pulls (jdbc_batch_common.py)
            # land parquet; API pulls that store the response verbatim (R-19 — obp_client.py)
            # land a single response.json. Same signal _check_checksum already uses to detect
            # payload-file-shaped partitions.
            if s3_io.exists(f"{partition_path}/response.json"):
                batch_df = spark.read.json(f"{partition_path}/response.json")
            else:
                batch_df = spark.read.parquet(partition_path)
            if not batch_df.columns:
                # An empty API response (e.g. a sandbox account/transaction list with 0 items)
                # has no inferable schema — Delta refuses to create a table from it
                # (DELTA_EMPTY_DATA). Nothing to append; transport integrity already passed
                # above, so this is a legitimate no-op promotion, not a quarantine.
                print(f"  {source}.{table}: 0 columns/rows in payload, nothing to append to Bronze")
            else:
                _check_schema_drift(spark, source, table, batch_df)
                batch_df.write.format("delta").mode("append").save(layer_path("bronze", source, table))

        return True

    except QuarantineReason as reason:
        notify_slack_failure(
            stage=f"Landing->Bronze promotion ({source}.{table})",
            detail=f"QUARANTINED: {reason}. Partition left in Landing at {partition_path}; "
                    f"Bronze untouched; pipeline continues on last-good Bronze (ADR-003).",
        )
        return False


def _promote_cdc(spark: SparkSession, source: str, table: str, events_df) -> None:
    """Exactly-once append: anti-join new events against what's already in Bronze on
    (pk_value, op, seq) before appending, so a redelivered poll never double-counts
    (R-36/R-37) — Bronze still ends up append-only/verbatim (D-05), just deduped."""
    bronze_path = layer_path("bronze", source, f"{table}_cdc")
    if s3_io.prefix_has_objects(bronze_path):
        existing = spark.read.format("delta").load(bronze_path).select("pk_value", "op", "seq")
        events_df = events_df.join(existing, on=["pk_value", "op", "seq"], how="left_anti")
    events_df.write.format("delta").mode("append").save(bronze_path)


# ---- standalone stage entrypoint (ADR-007 D7.3 — one node in pipeline/orchestrate.py's
# dependency graph, run after all 5 extraction stages) ----

# (source, table, mode) — mode "cdc" reads Landing at `<table>_cdc/dt=*` (cdc_common.py's
# poll shape); mode "batch" reads Landing at `<table>/dt=*` (jdbc_batch_common.py's / R-40's
# cdc_initial_snapshot.py's shape). Teradata gets BOTH: the ongoing CDC poll AND the
# one-time R-40 initial snapshot, which lands at the plain (non-`_cdc`) table name.
# Salesforce (source #4, ADR-006 Add #2) is "batch" only — Bulk API 2.0 + `SystemModstamp`
# watermark has no `_cdc_log`, so it needs no R-40 initial-snapshot companion either: the
# first extractor run (no watermark yet) IS the full initial pull, same as Postgres/MSSQL.
SOURCE_TABLES: dict[str, list[tuple[str, str]]] = {
    "postgres": [
        (t, "batch") for t in (
            "application", "bureau", "bureau_balance", "previous_application",
            "pos_cash_balance", "credit_card_balance", "installments_payments",
        )
    ],
    "mssql": [("paysim_transactions", "batch")],
    "salesforce": [
        (t, "batch") for t in
        ("contact", "account", "accountcontactrelation", "transaction", "district", "case")
    ],
    "teradata": [("bank_marketing", "cdc"), ("bank_marketing", "batch")],
    "obp": [("accounts", "batch"), ("transactions", "batch")],
}


def _landing_partitions(source: str, landing_table: str) -> list[str]:
    """Lists `dt=*` Landing partitions for (source, landing_table) not yet marked
    `_promoted`. Real S3 listing via `pipeline.common.s3_io` (2026-07-16 fix, staff-DE
    ruling) when AWS creds are present — sources whose extractor hasn't been migrated to
    real S3 I/O yet (Teradata CDC, OBP) simply enumerate zero partitions here rather than
    quarantining, since nothing of theirs is actually landed in S3."""
    landing_path = layer_path("landing", source, landing_table)
    return [
        f"{landing_path}/{dt_name}"
        for dt_name in s3_io.list_dt_partitions(landing_path)
        if not s3_io.exists(f"{landing_path}/{dt_name}/_promoted")
    ]


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("promotion_gate")
    promoted, quarantined = 0, 0
    for source, entries in SOURCE_TABLES.items():
        for table, mode in entries:
            landing_table = f"{table}_cdc" if mode == "cdc" else table
            for partition_path in _landing_partitions(source, landing_table):
                ok = promote_partition(spark, source, table, partition_path, mode)
                if ok:
                    # Marked ONLY on success — a quarantined partition stays pending so it
                    # keeps surfacing (and re-alerting) on every run until fixed, rather than
                    # being silently swept under the rug (ADR-003 "never silent").
                    s3_io.write_text(f"{partition_path}/_promoted", "")
                    promoted += 1
                else:
                    quarantined += 1
    print(f"promotion_gate complete: {promoted} partition(s) promoted, {quarantined} quarantined.")
    return 0


if __name__ == "__main__":
    # Only raise SystemExit on a real failure — Databricks' git-sourced spark_python_task
    # runner treats ANY raised SystemExit (even code 0) as a task failure (live-confirmed
    # 2026-07-16: promotion_gate genuinely promoted all 6 Salesforce partitions, 0
    # quarantined, real Bronze Delta verified in S3, yet the job run showed FAILED with
    # "SystemExit: 0" as the captured error). A bare `main()` call still exits 0 implicitly
    # on success; non-zero still signals failure correctly for CLI/orchestrator callers.
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
