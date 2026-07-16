"""Shared Gold-layer helpers — used by more than one mart, kept here once rather than
duplicated per mart file."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from pipeline.common.lake_paths import layer_path


def load_fx_rates(spark: SparkSession) -> DataFrame:
    """`dim_fx_rate` (D-12 static seed table) — the ONE place any Gold builder resolves a
    currency->MYR rate (ADR-005: no second resolution path)."""
    return spark.read.format("delta").load(layer_path("gold", "dim_fx_rate"))


def to_myr(spark: SparkSession, df: DataFrame, amount_col: str, currency_col: str, out_col: str) -> DataFrame:
    """D-12 — join the static FX seed table and add `out_col` = amount_col converted to MYR.
    A currency with no convertible rate (`rate_to_myr` NULL, e.g. `unitless`) produces a NULL
    `out_col` rather than a silently wrong number (R-14's conversion side, as distinct from
    the tag-completeness side enforced by `pipeline/gold/dq_currency_gate.py`)."""
    fx = load_fx_rates(spark).select(col("currency_code"), col("rate_to_myr"))
    return (
        df.join(fx, df[currency_col] == fx["currency_code"], "left")
        .withColumn(out_col, col(amount_col).cast("double") * col("rate_to_myr"))
        .drop("currency_code", "rate_to_myr")
    )


def latest_balance_per_account(spark: SparkSession, trans: DataFrame) -> DataFrame:
    """Berka's `account` table has no static balance column — each `trans` row carries the
    running post-transaction balance, so "current balance" = the latest `trans` row per
    account_id ((unverified) against the real .asc file — journey/03_DATA_REQUIREMENTS.md).

    Returns `current_balance` (native, CZK) AND `current_balance_myr` (D-12 reporting
    standard) — converted once here, the single point every caller (`mart_daily_flows.py`,
    `mart_cross_sell.py`) consumes, rather than each mart joining `dim_fx_rate` itself."""
    window = Window.partitionBy("account_id").orderBy(col("date").desc())
    latest = (
        trans.withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .select("account_id", col("balance").alias("current_balance"), "currency")
    )
    return to_myr(spark, latest, "current_balance", "currency", "current_balance_myr")
