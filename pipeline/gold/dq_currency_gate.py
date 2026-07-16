#!/usr/bin/env python3
"""R-14 (journey/06_DQ_PLAN.md) — Silver->Gold gate: block Gold build if any monetary
column lacks a currency tag (D-12, journey/05_STTM.md "Transform conventions"). Every
monetary column below must carry a non-null currency code in 100% of its Silver rows
before any Gold dim/fact/mart reads it — this is a completeness/tag check, not a
conversion check. Conversion happens once at the fact layer via
`pipeline/gold/common.py::to_myr`, against `pipeline/gold/dim_fx_rate.py`'s static seed
table.

`AMT_INCOME_TOTAL` (Home Credit, `sil_application`) is tagged `unitless` (D-12 exception,
@staff-data-engineer sign-off this session) — not currency-denominated (anonymized
competition data, real currency unknown), never summed/converted, but still carries a real
tag so this gate can pass honestly rather than special-casing an untagged column.

Runs after all 5 Bronze->Silver domains, before any Gold fact/mart that does money math
(`pipeline/orchestrate_config.yml`)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.common.lake_paths import layer_path

# (silver_table, monetary_column, currency_column) — every monetary column D-12 requires a
# currency tag for, across all 5 sources.
MONETARY_COLUMNS: list[tuple[str, str, str]] = [
    ("card_txn", "amount", "currency"),                        # PaySim, MYR
    ("trans", "amount", "currency"),                            # Berka, CZK
    ("trans", "balance", "currency"),                           # Berka, CZK
    ("campaign_response", "avg_yearly_balance", "currency"),    # Teradata, EUR
    ("obp_transactions", "amount", "currency"),                 # OBP, real per-txn (no Gold mart reads this yet — separate, unrelated gap)
    ("application", "AMT_INCOME_TOTAL", "currency"),            # Home Credit, tagged 'unitless' (D-12 exception)
]


class CurrencyTagMissing(Exception):
    """Raised to block Gold build — R-14."""


def check(spark: SparkSession) -> None:
    failures = []
    for table, money_col, currency_col in MONETARY_COLUMNS:
        df = spark.read.format("delta").load(layer_path("silver", table))
        if currency_col not in df.columns:
            failures.append(f"silver.{table}.{money_col} has no '{currency_col}' column at all")
            continue
        untagged = df.filter(col(money_col).isNotNull() & col(currency_col).isNull()).count()
        if untagged > 0:
            failures.append(f"silver.{table}.{money_col}: {untagged} row(s) with a value but no currency tag")
    if failures:
        raise CurrencyTagMissing(
            "R-14 currency-tag completeness failed, Gold build blocked:\n  " + "\n  ".join(failures)
        )
    print(f"R-14 currency-tag gate: {len(MONETARY_COLUMNS)} monetary column(s) checked, all tagged.")


def main() -> int:
    from pipeline.common.spark_session import get_spark

    check(get_spark("dq_currency_gate"))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
