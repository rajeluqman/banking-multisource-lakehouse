"""Shared Bronze -> Silver transform primitives (ADR-003 content-quality gate).

Every domain builder (pipeline/silver/silver_*.py, ADR-007 D7.1) composes these rather than
reimplementing MERGE, masking, orphan-quarantine, or CDC latest-state logic per file — kept
here once, shared across all 5 domain pipelines, not duplicated per file (ADR-007 D7.1).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, first, lit, length, row_number, substring, when

from pipeline.common.lake_paths import layer_path


def merge_upsert(spark: SparkSession, df: DataFrame, layer: str, table: str, pk_column: str | list[str]) -> None:
    """MERGE upsert keyed on pk_column — latest row per PK wins (journey/07_PIPELINE_SPEC.md
    idempotency section). Delta MERGE, not a naive overwrite, so re-running Silver doesn't
    lose history for PKs not present in the current Bronze read.

    `pk_column` accepts a list for tables with no single-column natural key (e.g. the HC-1
    monthly-snapshot tables, ADR-005 Add #5 — grain is `(sk_id_prev, months_balance)`, not one
    column) — composite MERGE condition, same semantics, not a new code path.

    Bronze can legitimately hold more than one row per PK once a table has been promoted more
    than once (each promotion batch appends, it doesn't replace) — Delta's MERGE INTO rejects a
    source with duplicate match keys outright (DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE),
    live-caught 2026-07-21 the first time a table was promoted a second time against non-empty
    Bronze. Dedup to one row per PK (latest `updated_at` wins, ties broken by `created_at`) before
    the MERGE — this is what actually delivers the "latest row per PK wins" the docstring already
    promised; the MERGE's own whenMatchedUpdateAll was never a substitute for it, since that clause
    only handles target-vs-source conflicts, not source-vs-source ones."""
    from delta.tables import DeltaTable

    pk_columns = [pk_column] if isinstance(pk_column, str) else pk_column
    merge_condition = " AND ".join(f"t.{c} = s.{c}" for c in pk_columns)

    if "updated_at" in df.columns:
        order_cols = [col("updated_at").desc()]
        if "created_at" in df.columns:
            order_cols.append(col("created_at").desc())
        window = Window.partitionBy(*pk_columns).orderBy(*order_cols)
        df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")

    target_path = layer_path(layer, table)
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").save(target_path)


def mask_last4(df: DataFrame, column: str, output_column: str | None = None) -> DataFrame:
    """D-07 — account/card numbers masked to last-4 only. A value shorter than 4 chars is
    masked to NULL entirely rather than partially revealing a short identifier.

    Masking must never land on an identity/join key (2026-07-17, BQ-10 live fix,
    journey/09_SECURITY_AND_ACCESS.md's D-07 clarification): pass `output_column` to write
    the masked value to a NEW derived column instead of overwriting `column` in place —
    required whenever `column` also serves as a table's MERGE key or an FK target, since
    last-4 masking is lossy (suffix collisions) and NULL-for-short-values breaks MERGE's
    own `NULL != NULL` re-run idempotency (a masked-to-NULL key duplicates every rebuild)."""
    target = output_column or column
    return df.withColumn(
        target,
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
    spark: SparkSession, source: str, bronze_table: str, silver_table: str, pk_column: str | list[str],
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
