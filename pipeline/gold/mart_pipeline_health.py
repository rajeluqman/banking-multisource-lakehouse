#!/usr/bin/env python3
"""`mart_pipeline_health` (BQ-10, mandatory — R-30) — per-source freshness, DQ fail count,
and Landing->Bronze->Silver->Gold row-count reconciliation. This mart IS the answer to
"can we trust the numbers," not a separate hidden report (journey/06_DQ_PLAN.md).

Grain: one row per (pipeline run, source) (journey/04_DATA_MODEL.md).

ADR-007 D7.3 (additive, does not change the row-count reconciliation logic below): also
surfaces the latest ORCHESTRATION status per source's Silver domain stage
(pipeline/orchestrate.py's run-status, written via pipeline/common/watermark.py's
`write_run_status`) — so this mart answers both "did the data counts reconcile" AND "did the
orchestrator's stage for this source actually complete," side by side.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

from pipeline.common.lake_paths import layer_path
from pipeline.common.watermark import read_run_status, read_watermark

MART_SCHEMA = StructType([
    StructField("run_ts", TimestampType()),
    StructField("source", StringType()),
    StructField("table", StringType()),
    StructField("bronze_row_count", LongType()),
    StructField("silver_row_count", LongType()),
    StructField("last_watermark", StringType()),
    StructField("reconciled", BooleanType()),
    StructField("orchestrator_stage", StringType()),
    StructField("orchestrator_status", StringType()),
    StructField("orchestrator_error", StringType()),
])

SOURCES_AND_TABLES = [
    ("postgres", "application"), ("mssql", "paysim_transactions"),
    ("salesforce", "contact"), ("teradata", "bank_marketing"), ("obp", "accounts"),
]

# source -> its Silver domain pipeline's orchestrator stage name (pipeline/orchestrate_config.yml,
# ADR-007 D7.1) — used ONLY to look up run-status, not part of the reconciliation logic itself.
SOURCE_SILVER_STAGE = {
    "postgres": "silver_sales", "mssql": "silver_fraud", "salesforce": "silver_crm",
    "teradata": "silver_marketing", "obp": "silver_core_banking",
}


def _row_count(spark: SparkSession, layer: str, source: str | None, table: str) -> int | None:
    # Bronze is partitioned by source (bronze/<source>/<table>); Silver is NOT — every
    # domain builder's merge_upsert (pipeline/silver/common.py) writes at silver/<table>
    # with no source segment, one Delta table per Silver entity regardless of which source
    # feeds it (R-30 — this mismatch used to make silver_row_count/reconciled wrong for
    # every source, not just one, until this fix).
    path = layer_path(layer, table) if source is None else layer_path(layer, source, table)
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return None  # table doesn't exist yet at this layer for this source — not an error


# Bronze table name -> its Silver table name, for the sources where they diverge (a plain
# `_cdc`-suffix strip covers Teradata; Salesforce's `contact` Bronze table becomes `client`
# at Silver per the STTM's Berka-native naming, ADR-006 Add #2).
BRONZE_TO_SILVER_TABLE = {
    "contact": "client", "paysim_transactions": "card_txn",
    "bank_marketing": "campaign_response", "accounts": "obp_accounts",
}


def build(spark: SparkSession) -> None:
    run_ts = dt.datetime.now(dt.timezone.utc)
    rows = []
    for source, table in SOURCES_AND_TABLES:
        bronze_count = _row_count(spark, "bronze", source, table)
        silver_table = BRONZE_TO_SILVER_TABLE.get(table.replace("_cdc", ""), table.replace("_cdc", ""))
        silver_count = _row_count(spark, "silver", None, silver_table)
        watermark_key = f"{table}_cdc_log" if source == "teradata" else table
        last_watermark = read_watermark(source, watermark_key)

        reconciled = (
            bronze_count is not None and silver_count is not None and silver_count <= bronze_count
        )  # Silver is <=Bronze (MERGE/dedup/quarantine can only shrink row count, never grow it)

        stage_run_status = read_run_status(SOURCE_SILVER_STAGE[source])  # ADR-007 D7.3 — additive

        rows.append(Row(
            run_ts=run_ts, source=source, table=table,
            bronze_row_count=bronze_count, silver_row_count=silver_count,
            last_watermark=last_watermark, reconciled=reconciled,
            orchestrator_stage=SOURCE_SILVER_STAGE[source],
            orchestrator_status=stage_run_status["status"] if stage_run_status else None,
            orchestrator_error=stage_run_status["error"] if stage_run_status else None,
        ))

    mart = spark.createDataFrame(rows, schema=MART_SCHEMA)
    mart.write.format("delta").mode("append").save(layer_path("gold", "mart_pipeline_health"))

    unreconciled = [r for r in rows if not r.reconciled]
    if unreconciled:
        from pipeline.common.alerts import notify_slack_failure

        notify_slack_failure(
            stage="mart_pipeline_health reconciliation",
            detail=f"{len(unreconciled)} source(s) failed Bronze->Silver reconciliation: "
                    f"{[r.source for r in unreconciled]} (R-30)",
        )


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("mart_pipeline_health"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
