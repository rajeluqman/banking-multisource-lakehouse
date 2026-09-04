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
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.common.lake_paths import layer_path
from pipeline.common.money import FX_RATE
from pipeline.common.repo_paths import find_seed_artifact

# `rate_to_myr` was DoubleType until 2026-09-04 (INC-0004). A float rate made every downstream
# MYR conversion in `pipeline/gold/common.py::to_myr` order-dependent under SUM. It is now
# DECIMAL(18,6) — see `pipeline/common/money.py::FX_RATE` for the scale choice and for the
# documented assumption that no journey/ or ADR document specifies a rate precision.
FX_RATE_SCHEMA = StructType([
    StructField("currency_code", StringType()),
    StructField("rate_to_myr", FX_RATE),
    StructField("rate_as_of", StringType()),
    StructField("note", StringType()),
])


def build(spark: SparkSession, fx_csv_path: str | None = None) -> None:
    fx_csv_path = fx_csv_path or find_seed_artifact("fx_rates.csv")
    with open(fx_csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        # Decimal(str(...)) not Decimal(float(...)) — parsing via float would bake in the
        # representation error this fix exists to remove before the value is ever stored.
        row["rate_to_myr"] = Decimal(str(row["rate_to_myr"])) if row["rate_to_myr"] not in (None, "") else None
    df = spark.createDataFrame(rows, schema=FX_RATE_SCHEMA)
    # `overwriteSchema` is REQUIRED, not decorative (2026-09-04, P3). Delta's `overwrite`
    # replaces data but ENFORCES the existing schema, so writing the new DECIMAL(18,6)
    # `rate_to_myr` over a table still carrying the old `double` raises a schema-mismatch
    # error — this stage runs before every Gold fact, so without this the whole Gold layer
    # is blocked on the first post-fix run. Safe here precisely because this table is a
    # static seed re-materialized from `seed/artifacts/fx_rates.csv` on every run: it holds
    # no history, so replacing its schema destroys nothing (contrast the append-mode facts,
    # which cannot be retyped this way at all and need a real rebuild).
    df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(layer_path("gold", "dim_fx_rate"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    build(get_spark("dim_fx_rate"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
