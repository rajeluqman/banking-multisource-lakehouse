"""Portable Spark session builder (ADR-002 D-01 Add #3 — no DLT, no notebook-only magic).

Two modes, one config switch (USE_UNITY_CATALOG env var) — the same PySpark code runs
against a Unity-Catalog-governed Databricks cluster during the canonical run, and against
plain local Spark (or Databricks Community Edition) with path-based Delta tables for the
free dev loop / after the disposable trial workspace is deleted. Nothing downstream
(pipeline/extract, pipeline/promote, pipeline/silver, pipeline/gold) should reference a UC
catalog name directly — always go through `table_ref()` below.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()


def use_unity_catalog() -> bool:
    return os.environ.get("USE_UNITY_CATALOG", "false").lower() == "true"


def table_ref(layer: str, table: str, catalog: str = "banking") -> str:
    """Resolves a table name for either mode. UC mode: `<catalog>.<layer>.<table>`
    (governed by the RBAC grants in journey/09_SECURITY_AND_ACCESS.md §3). Path-based mode:
    the caller uses this only as a Delta table NAME registered against a path via
    `DeltaTable.forPath` — the actual storage path comes from pipeline.common.lake_paths."""
    if use_unity_catalog():
        return f"{catalog}.{layer}.{table}"
    return f"{layer}_{table}"
