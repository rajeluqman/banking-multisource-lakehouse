"""Fixed-point monetary types — shared, not duplicated (ADR-007 D7.1).

`pipeline/gold/common.py::to_myr` converted currency with
`col(amount).cast("double") * col("rate_to_myr")`, and `dim_fx_rate.rate_to_myr` was itself
`DoubleType`. IEEE-754 addition is not associative, so a `SUM` over a double monetary column
depends on the order the values happen to be added in — which in Spark is partition order, not
part of the query semantics.

The error is small, confined to low digits, and never raises. It does not break a run; it makes
a run's totals non-reproducible. For a banking ledger an aggregate that changes between two runs
of identical input is an audit finding, and it silently undermines the per-run
source->Bronze->Silver->Gold reconciliation that `pipeline/gold/mart_pipeline_health.py`
(R-30, BQ-10) exists to publish.

The contract this module establishes is **exact decimal equality**, not tolerance comparison.
A tolerance is what you accept when a number is already approximate.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

#: Transaction-level monetary amounts and balances.
#:
#: **Scale 2** — currency minor units. Every monetary field in this build is a real-world
#: currency amount (Berka `trans.amount`/`balance` in CZK, PaySim `amount`, OBP transaction
#: amounts, Salesforce `amount__c`/`balance__c`).
#:
#: **Precision 18** — chosen for aggregation headroom, not for the values themselves. Spark
#: promotes precision on aggregation (`SUM` of DECIMAL(p, s) yields DECIMAL(p + 10, s)), so
#: DECIMAL(18,2) sums to DECIMAL(28,2) and stays clear of Spark's DECIMAL(38, ...) ceiling even
#: after a further join-and-aggregate. Starting at DECIMAL(38,2) would leave no headroom and
#: overflow to NULL — a silent failure of exactly the kind this module removes.
#:
#: ASSUMPTION (documented, not evidenced): no journey/ or ADR document specifies a monetary
#: precision/scale for this build. 18,2 is this module's defensible default; revisit if a
#: finance-side requirement ever states otherwise.
MONEY = DecimalType(18, 2)

#: FX rates. A rate is a ratio, not a currency amount, and needs more scale than one.
#:
#: ASSUMPTION (documented, not evidenced): `seed/artifacts/fx_rates.csv` and
#: `journey/04_DATA_MODEL.md` do not specify a rate precision. Scale 6 is the common market
#: convention for major pairs and is what `dim_fx_rate` now stores.
FX_RATE = DecimalType(18, 6)


def to_money(column: Column | str) -> Column:
    """Cast to the canonical monetary type.

    Cast from the source representation directly. Going via double first —
    `col.cast("double").cast(MONEY)` — reintroduces the representation error this module exists
    to remove, because the value is already approximate by the time it is rounded.
    """
    c = F.col(column) if isinstance(column, str) else column
    return c.cast(MONEY)


def to_fx_rate(column: Column | str) -> Column:
    """Cast to the canonical FX-rate type."""
    c = F.col(column) if isinstance(column, str) else column
    return c.cast(FX_RATE)


class FloatMoneyError(AssertionError):
    """Raised when a column that must hold money is a floating-point type."""


def assert_no_float_money(df: DataFrame, monetary_columns: tuple[str, ...]) -> None:
    """Guard for use at a layer boundary: fail if a declared monetary column is not decimal.

    Catches the realistic regression, which is not someone choosing `double` for money — it is a
    later transform re-deriving a monetary column through an arithmetic expression and silently
    landing back on `double`.
    """
    types = dict(df.dtypes)
    offenders = {c: types[c] for c in monetary_columns if c in types and not types[c].startswith("decimal")}
    if offenders:
        raise FloatMoneyError(
            f"monetary columns must be DECIMAL, found {offenders}. Float addition is not "
            "associative, so SUM over these is partition-order dependent."
        )
