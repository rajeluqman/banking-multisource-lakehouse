"""Shared Bronze -> Silver transform primitives (ADR-003 content-quality gate).

Every table's builder in build_silver.py composes these rather than reimplementing MERGE,
masking, or orphan-quarantine logic per table.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, length, lit, substring, when

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
