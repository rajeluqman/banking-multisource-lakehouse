#!/usr/bin/env python3
"""`dim_customer_xwalk` — the conformed dimension every other Gold model joins through
(ADR-005, D-04). Generated at SEED time (seed/build_xwalk.py), not derived here; this
module's job is to load that versioned seed artifact into a queryable Delta table so Gold
builds have a stable table to join against instead of re-reading a CSV each time.

Grain: one row per (customer_id, source_system) pair (journey/04_DATA_MODEL.md).
"""

from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.common.lake_paths import layer_path

XWALK_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("source_system", StringType()),
    StructField("native_key", StringType()),
    StructField("source_priority_rank", StringType()),
])

# 2026-07-17: a plain "seed/artifacts/..." relative default assumed CWD == repo root, true for
# a local dev-loop run but NOT for a git_source Databricks task (real, reproducible failure —
# job run 127330185225331 — FileNotFoundError the first time this ever ran on the cluster).
# Anchor to this file's own location instead of CWD.
_REPO_ROOT = Path(__file__).resolve().parents[2]


def build(spark: SparkSession, xwalk_csv_path: str | None = None) -> None:
    xwalk_csv_path = xwalk_csv_path or str(_REPO_ROOT / "seed/artifacts/dim_customer_xwalk.csv")
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
