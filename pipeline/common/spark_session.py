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
    if use_unity_catalog():
        # Databricks runtime bundles the Delta JVM jars already — nothing more to configure.
        return builder.getOrCreate()
    # Local/dev-loop Spark (D-14) has no Delta jar on the classpath by default; the delta-spark
    # pip package only ships the Python side. Resolve the matching JVM jar via Maven coordinates
    # (io.delta:delta-spark_2.12:3.2.1 matches pyspark==3.5.3 / delta-spark==3.2.1 pinned in
    # requirements.txt). Same story for the Postgres/MS SQL JDBC drivers postgres_extract.py/
    # mssql_extract.py need — a Databricks cluster has these preinstalled, vanilla pip pyspark
    # does not.
    from delta import configure_spark_with_delta_pip
    jdbc_packages = ["org.postgresql:postgresql:42.7.4", "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"]
    return configure_spark_with_delta_pip(builder, extra_packages=jdbc_packages).getOrCreate()


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
