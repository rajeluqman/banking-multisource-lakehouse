"""Shared watermark-batch JDBC extraction logic (ADR-004 — Postgres/MSSQL stay batch-first,
unchanged by the ADR-006 amendment). Used by postgres_extract.py and mssql_extract.py —
those differ only in JDBC URL/driver/table list, not in extraction logic, so that logic
lives here once (R-26 overlap window, manifest + `_SUCCESS`, watermark advance).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json

from pyspark.sql import DataFrame, SparkSession

from pipeline.common import s3_io
from pipeline.common.lake_paths import layer_path
from pipeline.common.run_interval import interval_window, logical_date
from pipeline.common.watermark import read_watermark, write_watermark

OVERLAP_WINDOW = dt.timedelta(minutes=5)  # R-26 — catches late/out-of-order drip-feed writes


def extract_table(
    spark: SparkSession,
    source: str,
    table: str,
    jdbc_url: str,
    jdbc_properties: dict,
    updated_at_column: str = "updated_at",
    full_backfill: bool = False,
) -> str:
    """Pulls rows where `updated_at_column` > (last watermark - overlap window), lands them
    as parquet under Landing `dt=<today>`, writes a manifest + `_SUCCESS` marker, and
    advances the watermark. Returns the Landing partition path written.

    `full_backfill=True` (ADR-007 D7.4 Strategy 1) forces the full-pull branch below
    regardless of existing watermark state — makes the already-present first-run behavior an
    EXPLICIT, deliberate re-pull instead of requiring someone to manually delete watermark
    state to get the same effect. The watermark still advances normally afterward, same as
    any other run.

    Idempotent: re-running with the same watermark re-pulls the same window — downstream
    dedup happens at the Silver MERGE (PK + updated_at), not here (journey/07_PIPELINE_SPEC.md).

    Predicate precedence (staff-DE ruling, contract-compliance fix, 2026-07-20):
      1. `full_backfill=True` -> unbounded `1=1` (ADR-007 D7.4 Strategy 1), unchanged.
      2. `DATA_INTERVAL_START`/`DATA_INTERVAL_END` both set (Airflow-driven run, incl.
         backfills) -> BOUNDED window `[start - overlap, end)`, keyed to the logical date,
         not the lake watermark — this is what makes a backfill a pure function of the
         logical date (PIPELINE_SIDE_CONTRACT.md §3, ADR-011 D11.5). The lake watermark is
         deliberately NOT read or written in this mode: reading it would reintroduce the
         non-determinism this fix removes, and writing `end` risks regressing the watermark
         if backfills run out of chronological order, corrupting the next watermark-mode run.
      3. Neither set (local dev-loop / direct-CLI, no Airflow above this run) -> the
         pre-existing lake-watermark high-water-mark mode, unchanged.
    """
    window = None if full_backfill else interval_window(OVERLAP_WINDOW)
    if full_backfill:
        predicate = "1=1"
    elif window is not None:
        effective_start, effective_end = window
        predicate = (
            f"{updated_at_column} > '{effective_start.isoformat()}' "
            f"AND {updated_at_column} < '{effective_end.isoformat()}'"
        )
    else:
        last_watermark = read_watermark(source, table)
        if last_watermark is not None:
            effective_start = dt.datetime.fromisoformat(last_watermark) - OVERLAP_WINDOW
            predicate = f"{updated_at_column} > '{effective_start.isoformat()}'"
        else:
            predicate = "1=1"  # first run — full pull

    df: DataFrame = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"(SELECT * FROM {table} WHERE {predicate}) AS t")
        # Without an explicit fetchsize, Postgres' JDBC driver buffers the ENTIRE result set
        # client-side before Spark sees any rows (a well-documented Postgres JDBC trap) — fine
        # for narrow/small tables, but live-caught (2026-07-17) crashing the JVM on
        # previous_application (1.67M rows x 37 columns) even though bureau (a similar row
        # count but far fewer columns) was fine. fetchsize enables server-side cursor batching
        # regardless of driver (MSSQL/Postgres both honor it), bounding client memory.
        .option("fetchsize", 10_000)
        .options(**jdbc_properties)
        .load()
    )

    run_date = logical_date()
    partition_path = layer_path("landing", source, table, f"dt={run_date}")
    # Spark's native writer needs S3A (hadoop-aws) to target s3:// directly — local-mode Spark
    # doesn't have that wired in (pipeline/common/spark_session.py only adds Delta + JDBC-driver
    # Maven packages), so write to a local staging dir first (same /tmp/s3_staging/ convention
    # promotion_gate.py already uses for CDC/OBP), then push the bytes up via s3_io/boto3 —
    # mirrors the Salesforce S3 fix (2026-07-17, staff-DE ruling): avoids a version-fragile
    # hadoop-aws/aws-java-sdk-bundle JAR stack, and avoids collecting large tables (PaySim 6.36M
    # rows, Home Credit installments_payments ~13.6M rows) into a pandas/driver buffer, since
    # Spark still writes/spills the parquet locally, not through a Python-side collect.
    local_dir = partition_path.replace("s3://", "/tmp/s3_staging/") if s3_io.is_s3(partition_path) else partition_path
    df.write.mode("overwrite").parquet(local_dir)

    row_count = df.count()
    _write_manifest(local_dir, source, table, row_count)
    if s3_io.is_s3(partition_path):
        s3_io.upload_dir(local_dir, partition_path)
    if window is None:
        # Only advance the lake watermark in fallback mode (see precedence note above) —
        # an interval-mode run must not mutate state a later watermark-mode run depends on.
        write_watermark(source, table, dt.datetime.now(dt.timezone.utc).isoformat())

    return partition_path


def _write_manifest(local_dir: str, source: str, table: str, row_count: int) -> None:
    """Manifest + `_SUCCESS` — the transport-integrity evidence the Landing->Bronze
    promotion gate checks (ADR-003/D-15). Written LAST, only after the data write above
    completed, so a mid-write failure leaves no `_SUCCESS` and the gate correctly treats
    the partition as incomplete. Written to the same local dir the parquet write used —
    `s3_io.upload_dir()` pushes both up to S3 together (see extract_table above)."""
    manifest = {
        "source": source, "table": table, "row_count": row_count,
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    manifest_json = json.dumps(manifest)
    manifest["checksum"] = hashlib.sha256(manifest_json.encode()).hexdigest()

    s3_io.write_text(f"{local_dir}/_manifest.json", json.dumps(manifest))
    s3_io.write_text(f"{local_dir}/_SUCCESS", "")
