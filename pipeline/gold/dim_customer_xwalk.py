#!/usr/bin/env python3
"""`dim_customer_xwalk` — the conformed dimension every other Gold model joins through
(ADR-005, D-04). Generated at SEED time (seed/build_xwalk.py), not derived here; this
module's job is to load that versioned seed artifact into a queryable Delta table so Gold
builds have a stable table to join against instead of re-reading a CSV each time.

Grain: one row per (customer_id, source_system) pair (journey/04_DATA_MODEL.md).

**Two seed-artifact paths (ADR-005 Addendum #3, 2026-07-17)**: the D-14 dev-loop CSV
(`seed/artifacts/dim_customer_xwalk.csv`, ~12MB/345,857 rows) stays git-committed — small
enough, same pattern as `fx_rates.csv`. The D-14 CANONICAL full-population artifact
(~7.2M rows, ~278MB at real PaySim scale) is NOT git-committed — no Git LFS is configured
in this repo and GitHub hard-rejects any single file over 100MB, so committing it is not
just undesirable but mechanically impossible. It lives instead as a Delta table in the S3
data plane, under a seed-artifact prefix, generated once (locally, `seed/build_xwalk.py`'s
memory-safe streaming build, then written to S3) rather than shipped through git_source —
this does NOT touch the git_source "ship code, not data" boundary (ADR-002 Add #6,
ADR-008): `dim_customer_xwalk` is Gold DATA, not pipeline code, and belongs in the S3 data
plane like every other Gold table, the same as this file already treats `dim_fx_rate`.
Preference order below: read the full-scale S3 artifact if it exists, else fall back to
the small git CSV — so the dev loop (no S3 artifact ever written there) is unaffected."""

from __future__ import annotations

import csv

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.common.lake_paths import layer_path
from pipeline.common.repo_paths import find_seed_artifact

XWALK_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("source_system", StringType()),
    StructField("native_key", StringType()),
    StructField("source_priority_rank", StringType()),
])

def build(spark: SparkSession, xwalk_csv_path: str | None = None) -> None:
    full_scale_path = layer_path("gold", "_seed_artifacts", "dim_customer_xwalk")
    if xwalk_csv_path is None and DeltaTable.isDeltaTable(spark, full_scale_path):
        df = spark.read.format("delta").load(full_scale_path)
    else:
        xwalk_csv_path = xwalk_csv_path or find_seed_artifact("dim_customer_xwalk.csv")
        with open(xwalk_csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        df = spark.createDataFrame(rows, schema=XWALK_SCHEMA)
    df.write.format("delta").mode("overwrite").save(layer_path("gold", "dim_customer_xwalk"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("dim_customer_xwalk"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
