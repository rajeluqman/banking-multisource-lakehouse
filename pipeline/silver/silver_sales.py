#!/usr/bin/env python3
"""Bronze -> Silver, Home Credit ("Sales / Loan Dept") domain (ADR-007 D7.1 — split out of
the former build_silver.py so a Home Credit-specific failure never blocks the other 4
domains). Covers `application` (column pruning), `bureau` (orphan quarantine, R-03), and
`previous_application` (generic passthrough).

Not executed against live Bronze data this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path
from pipeline.silver.common import build_simple_table, merge_upsert, orphan_quarantine


def build_sil_application(spark: SparkSession) -> None:
    """Home Credit `application` — Bronze keeps ALL ~122 anonymized columns verbatim (R-02);
    Silver prunes to the STTM-selected set (journey/05_STTM.md), everything else dropped
    here, not silently carried forward."""
    KEEP_COLUMNS = [
        "SK_ID_CURR", "TARGET", "AMT_INCOME_TOTAL", "NAME_INCOME_TYPE", "ORGANIZATION_TYPE",
        "created_at", "updated_at", "is_deleted",
    ]
    df = spark.read.format("delta").load(layer_path("bronze", "postgres", "application"))
    pruned = df.select(*[c for c in KEEP_COLUMNS if c in df.columns])
    merge_upsert(spark, pruned, "silver", "application", "SK_ID_CURR")


def build_sil_bureau(spark: SparkSession) -> None:
    """R-03 — orphan `bureau` rows (no matching `application.SK_ID_CURR`) go to quarantine,
    counted and reported, never silently dropped."""
    bureau = spark.read.format("delta").load(layer_path("bronze", "postgres", "bureau"))
    application = spark.read.format("delta").load(layer_path("silver", "application"))
    clean, orphans = orphan_quarantine(bureau, "SK_ID_CURR", application, "SK_ID_CURR")
    merge_upsert(spark, clean, "silver", "bureau", "SK_ID_BUREAU")
    if orphans.count() > 0:
        orphans.write.format("delta").mode("append").save(layer_path("silver", "_quarantine_bureau_orphans"))
        print(f"WARNING: {orphans.count()} bureau rows quarantined as orphan FKs (R-03)")


SIMPLE_TABLES = [
    # (source, bronze_table, silver_table, pk_column, mask_columns)
    ("postgres", "previous_application", "previous_application", "SK_ID_PREV", []),
]


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("silver_sales")
    build_sil_application(spark)
    build_sil_bureau(spark)
    for source, bronze_table, silver_table, pk_column, mask_columns in SIMPLE_TABLES:
        build_simple_table(spark, source, bronze_table, silver_table, pk_column, mask_columns)
    print(f"silver_sales complete: {2 + len(SIMPLE_TABLES)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
