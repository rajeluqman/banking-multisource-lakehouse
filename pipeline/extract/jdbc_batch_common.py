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

from pipeline.common.lake_paths import layer_path
from pipeline.common.watermark import read_watermark, write_watermark

OVERLAP_WINDOW = dt.timedelta(minutes=5)  # R-26 — catches late/out-of-order drip-feed writes


def extract_table(
    spark: SparkSession,
    source: str,
    table: str,
    jdbc_url: str,
    jdbc_properties: dict,
    updated_at_column: str = "updated_at",
) -> str:
    """Pulls rows where `updated_at_column` > (last watermark - overlap window), lands them
    as parquet under Landing `dt=<today>`, writes a manifest + `_SUCCESS` marker, and
    advances the watermark. Returns the Landing partition path written.

    Idempotent: re-running with the same watermark re-pulls the same window — downstream
    dedup happens at the Silver MERGE (PK + updated_at), not here (journey/07_PIPELINE_SPEC.md).
    """
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
        .options(**jdbc_properties)
        .load()
    )

    run_date = dt.date.today().isoformat()
    partition_path = layer_path("landing", source, table, f"dt={run_date}")
    df.write.mode("overwrite").parquet(partition_path)

    row_count = df.count()
    new_watermark = dt.datetime.now(dt.timezone.utc).isoformat()
    _write_manifest(partition_path, source, table, row_count, spark)
    write_watermark(source, table, new_watermark)

    return partition_path


def _write_manifest(partition_path: str, source: str, table: str, row_count: int, spark: SparkSession) -> None:
    """Manifest + `_SUCCESS` — the transport-integrity evidence the Landing->Bronze
    promotion gate checks (ADR-003/D-15). Written LAST, only after the data write above
    completed, so a mid-write failure leaves no `_SUCCESS` and the gate correctly treats
    the partition as incomplete."""
    manifest = {
        "source": source, "table": table, "row_count": row_count,
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    manifest_json = json.dumps(manifest)
    manifest["checksum"] = hashlib.sha256(manifest_json.encode()).hexdigest()

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    Path = sc._jvm.org.apache.hadoop.fs.Path

    def _write(name: str, content: str) -> None:
        out = fs.create(Path(f"{partition_path}/{name}"))
        out.write(bytearray(content.encode("utf-8")))
        out.close()

    _write("_manifest.json", json.dumps(manifest))
    _write("_SUCCESS", "")
