#!/usr/bin/env python3
"""`dim_date` — one row per calendar date (journey/04_DATA_MODEL.md). Spans the seeded data
range so every fact's date FK resolves; no SCD (a date row never changes)."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from pipeline.common.lake_paths import layer_path

SCHEMA = StructType([
    StructField("date_key", DateType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
    StructField("month_name", StringType()),
    StructField("day_of_week", StringType()),
])


def build(spark: SparkSession, start: dt.date, end: dt.date) -> None:
    rows = []
    d = start
    while d <= end:
        rows.append((d, d.year, d.month, d.day, d.strftime("%B"), d.strftime("%A")))
        d += dt.timedelta(days=1)
    df = spark.createDataFrame(rows, schema=SCHEMA)
    df.write.format("delta").mode("overwrite").save(layer_path("gold", "dim_date"))


def main() -> int:
    from pipeline.common.spark_session import get_spark

    today = dt.date.today()
    build(get_spark("dim_date"), start=today - dt.timedelta(days=3650), end=today)
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
