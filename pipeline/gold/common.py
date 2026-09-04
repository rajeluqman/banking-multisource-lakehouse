"""Shared Gold-layer helpers — used by more than one mart, kept here once rather than
duplicated per mart file."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path
from pipeline.common.money import MONEY, assert_no_float_money, to_fx_rate, to_money
from pipeline.common.ordering import latest_row_per_key


def load_fx_rates(spark: SparkSession) -> DataFrame:
    """`dim_fx_rate` (D-12 static seed table) — the ONE place any Gold builder resolves a
    currency->MYR rate (ADR-005: no second resolution path)."""
    return spark.read.format("delta").load(layer_path("gold", "dim_fx_rate"))


def to_myr(spark: SparkSession, df: DataFrame, amount_col: str, currency_col: str, out_col: str) -> DataFrame:
    """D-12 — join the static FX seed table and add `out_col` = amount_col converted to MYR.
    A currency with no convertible rate (`rate_to_myr` NULL, e.g. `unitless`) produces a NULL
    `out_col` rather than a silently wrong number (R-14's conversion side, as distinct from
    the tag-completeness side enforced by `pipeline/gold/dq_currency_gate.py`).

    PRECISION FIX (2026-09-04, INC-0004). This previously computed
    `col(amount_col).cast("double") * col("rate_to_myr")` with `rate_to_myr` itself stored as
    `DoubleType`. IEEE-754 addition is not associative, so any `SUM` over the resulting column
    depended on the order Spark happened to add the values in — i.e. on partitioning, which is
    not part of the query semantics. Nothing raised; the totals were simply not reproducible,
    which quietly undermines the per-run row/amount reconciliation `mart_pipeline_health.py`
    (R-30, BQ-10) publishes. Both sides are now fixed-point: amount cast to DECIMAL(18,2), rate
    to DECIMAL(18,6), product rounded back to DECIMAL(18,2) (currency minor units). The rate is
    re-cast here as well as at the seed, so a Gold table written before this fix cannot
    reintroduce a double into the multiply. NULL-rate behaviour is unchanged — decimal NULL
    propagates exactly as double NULL did."""
    # REGRESSION GUARD (2026-09-04, P3). `to_myr` is the single Silver->Gold monetary
    # boundary (ADR-005: one resolution path), which makes it the one place worth asserting
    # at. `to_money` below would happily accept a double and round it to 2dp — that recovers
    # ordinary amounts and hides the defect, which is exactly how the Silver-side `double`
    # casts survived the INC-0004 fix. Failing closed here means a future transform that
    # re-derives a monetary column back into `double` raises at the boundary instead of
    # silently degrading the totals downstream of it.
    assert_no_float_money(df, (amount_col,))
    fx = load_fx_rates(spark).select(col("currency_code"), to_fx_rate("rate_to_myr").alias("rate_to_myr"))
    return (
        df.join(fx, df[currency_col] == fx["currency_code"], "left")
        .withColumn(out_col, (to_money(amount_col) * col("rate_to_myr")).cast(MONEY))
        .drop("currency_code", "rate_to_myr")
    )


def latest_balance_per_account(spark: SparkSession, trans: DataFrame) -> DataFrame:
    """Berka's `account` table has no static balance column — each `trans` row carries the
    running post-transaction balance, so "current balance" = the latest `trans` row per
    account_id ((unverified) against the real .asc file — journey/03_DATA_REQUIREMENTS.md).

    Returns `current_balance` (native, CZK) AND `current_balance_myr` (D-12 reporting
    standard) — converted once here, the single point every caller (`mart_daily_flows.py`,
    `mart_cross_sell.py`) consumes, rather than each mart joining `dim_fx_rate` itself.

    DETERMINISM FIX (2026-09-04, INC-0003). Ordering was `orderBy(col("date").desc())` alone.
    Berka's `trans.date` is day-granular and an account routinely has several transactions on
    the same day, so the "latest" row was a tie broken by executor order — meaning an account's
    reported current balance, and every mart derived from it, could differ between two runs over
    identical Silver. Ordering now closes with a content-derived tie-break
    (`pipeline/common/ordering.py`). Grain is unchanged: still one row per `account_id`
    (journey/04_DATA_MODEL.md). Where several transactions share the account's last date the
    survivor remains arbitrary in MEANING — the source carries no intra-day sequence this helper
    can see — but it is now the same one on every run."""
    latest = latest_row_per_key(trans, ["account_id"], extra_order=[col("date").desc()]).select(
        "account_id", to_money("balance").alias("current_balance"), "currency"
    )
    return to_myr(spark, latest, "current_balance", "currency", "current_balance_myr")
