"""Shared Gold-layer helpers — used by more than one mart, kept here once rather than
duplicated per mart file."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def latest_balance_per_account(trans: DataFrame) -> DataFrame:
    """Berka's `account` table has no static balance column — each `trans` row carries the
    running post-transaction balance, so "current balance" = the latest `trans` row per
    account_id ((unverified) against the real .asc file — journey/03_DATA_REQUIREMENTS.md)."""
    window = Window.partitionBy("account_id").orderBy(col("date").desc())
    return (
        trans.withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .select("account_id", col("balance").alias("current_balance"))
    )
