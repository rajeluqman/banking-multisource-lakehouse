#!/usr/bin/env python3
"""`dim_fx_rate` — static FX seed table (D-12, journey/05_STTM.md "Transform conventions").
Generated at SEED time (`seed/artifacts/fx_rates.csv`), not derived here — same pattern as
`dim_customer_xwalk.py`: this module's job is to load the versioned seed artifact into a
queryable Delta table so Gold builds have a stable table to join against instead of
re-reading a CSV each time.

Grain: one row per currency_code (journey/04_DATA_MODEL.md; ADR-005 addendum #1 — a
conformed dimension implementing locked D-12, not new scope). `rate_to_myr` is a static,
illustrative rate, NOT a live BNM OpenAPI feed (D-12 explicitly makes that optional and
never a build dependency) — `rate_as_of` is metadata only, not part of the join/PK, so this
stays a true static seed table rather than a date-versioned one.

A currency with a NULL `rate_to_myr` (e.g. `unitless` — Home Credit's `AMT_INCOME_TOTAL`,
D-12 exception) is a deliberate non-convertible sentinel: `pipeline/gold/common.py::to_myr`
produces a NULL converted amount for it rather than a silently wrong number."""

from __future__ import annotations

import csv

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from pipeline.common.lake_paths import layer_path

FX_RATE_SCHEMA = StructType([
    StructField("currency_code", StringType()),
    StructField("rate_to_myr", DoubleType()),
    StructField("rate_as_of", StringType()),
    StructField("note", StringType()),
])


def build(spark: SparkSession, fx_csv_path: str = "seed/artifacts/fx_rates.csv") -> None:
    with open(fx_csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["rate_to_myr"] = float(row["rate_to_myr"]) if row["rate_to_myr"] not in (None, "") else None
    df = spark.createDataFrame(rows, schema=FX_RATE_SCHEMA)
    df.write.format("delta").mode("overwrite").save(layer_path("gold", "dim_fx_rate"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("dim_fx_rate"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
