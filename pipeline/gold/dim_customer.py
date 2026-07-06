#!/usr/bin/env python3
"""`dim_customer` — the golden record (ADR-005). Type 1 SCD (overwrite each run — no
history tracking in v1, named as deliberately out in journey/04_DATA_MODEL.md).

Survivorship: source priority CRM(1, sil_client via SAP HANA) > core(2, OBP) >
loans(3, sil_application) > cards(4, sil_card_txn); within a customer_id, attributes come
from the highest-priority source that HAS that customer (a customer only in cards, priority
4, still gets a row — survivorship picks the best AVAILABLE source, it doesn't require CRM
presence).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

from pipeline.common.lake_paths import layer_path


def _candidates(xwalk: DataFrame, client: DataFrame, application: DataFrame) -> DataFrame:
    """One row per (customer_id, source) with whatever demographic attributes that source
    can supply — a common column set across sources, null where a source doesn't carry it."""
    berka_candidates = (
        xwalk.filter(col("source_system") == "berka")
        .join(client, "native_key", "left")
        .select("customer_id", "source_priority_rank", "birth_date", "gender", "district_id")
    )
    home_credit_candidates = (
        xwalk.filter(col("source_system") == "home_credit")
        .join(application, "native_key", "left")
        .select(
            "customer_id", "source_priority_rank",
            lit(None).cast("string").alias("birth_date"),
            lit(None).cast("string").alias("gender"),
            lit(None).cast("string").alias("district_id"),
        )
    )
    return berka_candidates.unionByName(home_credit_candidates)


def build(spark: SparkSession) -> None:
    xwalk = spark.read.format("delta").load(layer_path("gold", "dim_customer_xwalk"))
    client = spark.read.format("delta").load(layer_path("silver", "client")) \
        .select(col("client_id").alias("native_key"), "birth_date", "gender", "district_id")
    application = spark.read.format("delta").load(layer_path("silver", "application")) \
        .select(col("SK_ID_CURR").cast("string").alias("native_key"))

    candidates = _candidates(xwalk, client, application)

    window = Window.partitionBy("customer_id").orderBy(col("source_priority_rank").asc())
    survivor = (
        candidates.withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )

    dim = xwalk.select("customer_id").distinct().join(survivor, "customer_id", "left")
    dim.write.format("delta").mode("overwrite").save(layer_path("gold", "dim_customer"))


if __name__ == "__main__":
    from pipeline.common.spark_session import get_spark

    build(get_spark("dim_customer"))
