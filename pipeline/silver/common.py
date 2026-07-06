"""Shared Bronze -> Silver transform primitives (ADR-003 content-quality gate).

Every domain builder (pipeline/silver/silver_*.py, ADR-007 D7.1) composes these rather than
reimplementing MERGE, masking, orphan-quarantine, or CDC latest-state logic per file — kept
here once, shared across all 5 domain pipelines, not duplicated per file (ADR-007 D7.1).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, first, lit, length, substring, when

from pipeline.common.lake_paths import layer_path


def merge_upsert(spark: SparkSession, df: DataFrame, layer: str, table: str, pk_column: str) -> None:
    """MERGE upsert keyed on pk_column — latest row per PK wins (journey/07_PIPELINE_SPEC.md
    idempotency section). Delta MERGE, not a naive overwrite, so re-running Silver doesn't
    lose history for PKs not present in the current Bronze read."""
    from delta.tables import DeltaTable

    target_path = layer_path(layer, table)
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(df.alias("s"), f"t.{pk_column} = s.{pk_column}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").save(target_path)


def mask_last4(df: DataFrame, column: str) -> DataFrame:
    """D-07 — account/card numbers masked to last-4 only. A value shorter than 4 chars is
    masked to NULL entirely rather than partially revealing a short identifier."""
    return df.withColumn(
        column,
        when(length(col(column)) >= 4, substring(col(column), -4, 4)).otherwise(lit(None)),
    )


def orphan_quarantine(df: DataFrame, fk_column: str, parent_df: DataFrame, parent_key: str) -> tuple[DataFrame, DataFrame]:
    """R-03 — rows whose FK has no matching parent go to a quarantine set, counted and
    reported, never silently dropped. Returns (clean_rows, orphan_rows)."""
    parent_keys = parent_df.select(col(parent_key).alias("_parent_key")).distinct()
    joined = df.join(parent_keys, df[fk_column] == col("_parent_key"), how="left")
    clean = joined.filter(col("_parent_key").isNotNull()).drop("_parent_key")
    orphans = joined.filter(col("_parent_key").isNull()).drop("_parent_key")
    return clean, orphans


def build_simple_table(
    spark: SparkSession, source: str, bronze_table: str, silver_table: str, pk_column: str,
    mask_columns: list[str] | None = None,
) -> DataFrame:
    """Generic passthrough builder for tables with no dedicated transform rule (naming +
    masking only, per journey/05_STTM.md). Read the latest Bronze state (batch tables: one
    row per PK already; CDC tables: replay op-log to latest-state per PK first — see
    `latest_state_from_cdc_log`), apply any PII masking, MERGE into Silver. Shared across
    silver_sales.py/silver_crm.py/silver_core_banking.py (ADR-007 D7.1) — not duplicated."""
    bronze_path = layer_path("bronze", source, bronze_table)
    df = spark.read.format("delta").load(bronze_path)
    for column in mask_columns or []:
        if column in df.columns:
            df = mask_last4(df, column)
    merge_upsert(spark, df, "silver", silver_table, pk_column)
    return df


def latest_state_from_cdc_log(spark: SparkSession, source: str, cdc_bronze_table: str, pk_column: str) -> DataFrame:
    """CDC Bronze holds a raw op-log (I/U/D events, ADR-006 D6.3) — Silver needs the LATEST
    state per pk_value, with 'D' ops excluded (soft-delete semantics, D-06; hard-delete
    replay is a Fasa C-later CDC concern per R-25, unchanged by ADR-006). Shared across
    silver_crm.py/silver_marketing.py (ADR-007 D7.1) — not duplicated."""
    events = spark.read.format("delta").load(layer_path("bronze", source, cdc_bronze_table))
    latest = (
        events.orderBy(col("seq").desc())
        .groupBy("pk_value")
        .agg(first("op").alias("latest_op"), first("changed_at").alias("latest_changed_at"))
    )
    return latest.filter(col("latest_op") != "D").withColumnRenamed("pk_value", pk_column)
