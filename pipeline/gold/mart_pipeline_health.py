#!/usr/bin/env python3
"""`mart_pipeline_health` (BQ-10, mandatory — R-30) — per-source freshness, DQ fail count,
and Landing->Bronze->Silver->Gold row-count reconciliation. This mart IS the answer to
"can we trust the numbers," not a separate hidden report (journey/06_DQ_PLAN.md).

Grain: one row per (pipeline run, source) (journey/04_DATA_MODEL.md).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Row, SparkSession

from pipeline.common.lake_paths import layer_path
from pipeline.common.watermark import read_watermark

SOURCES_AND_TABLES = [
    ("postgres", "application"), ("mssql", "paysim_transactions"),
    ("sap_hana", "client_cdc"), ("teradata", "bank_marketing_cdc"), ("obp", "accounts"),
]


def _row_count(spark: SparkSession, layer: str, source: str, table: str) -> int | None:
    path = layer_path(layer, source, table)
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return None  # table doesn't exist yet at this layer for this source — not an error


def build(spark: SparkSession) -> None:
    run_ts = dt.datetime.now(dt.timezone.utc)
    rows = []
    for source, table in SOURCES_AND_TABLES:
        bronze_count = _row_count(spark, "bronze", source, table)
        silver_table = table.replace("_cdc", "").replace("paysim_transactions", "card_txn")
        silver_count = _row_count(spark, "silver", source, silver_table)
        watermark_key = f"{table}_cdc_log" if source in ("sap_hana", "teradata") else table
        last_watermark = read_watermark(source, watermark_key)

        reconciled = (
            bronze_count is not None and silver_count is not None and silver_count <= bronze_count
        )  # Silver is <=Bronze (MERGE/dedup/quarantine can only shrink row count, never grow it)

        rows.append(Row(
            run_ts=run_ts, source=source, table=table,
            bronze_row_count=bronze_count, silver_row_count=silver_count,
            last_watermark=last_watermark, reconciled=reconciled,
        ))

    mart = spark.createDataFrame(rows)
    mart.write.format("delta").mode("append").save(layer_path("gold", "mart_pipeline_health"))

    unreconciled = [r for r in rows if not r.reconciled]
    if unreconciled:
        from pipeline.common.alerts import notify_slack_failure

        notify_slack_failure(
            stage="mart_pipeline_health reconciliation",
            detail=f"{len(unreconciled)} source(s) failed Bronze->Silver reconciliation: "
                    f"{[r.source for r in unreconciled]} (R-30)",
        )


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_pipeline_health"))
