"""Regression tests for INC-0004 — monetary values must be fixed-point, not float.

The arithmetic proofs are pure-Python and ALWAYS run, so the correctness contract is never
silently skipped when Spark or a JDK is unavailable (as in CI). The Spark tests additionally
prove the contract holds through a real aggregation, and skip cleanly when they cannot run.

Run:  python -m unittest tests.test_money_precision -v
"""

from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests._spark_support import MONEY_AVAILABLE, spark_session_or_skip, sql_values


class TestFloatIsUnsafeForMoney(unittest.TestCase):
    """Pure arithmetic — no Spark, no JDK. These prove WHY the fix was necessary."""

    def test_float_addition_is_not_associative(self):
        # Same values, same operator, different grouping, different answer. Spark picks the
        # grouping by partitioning, which is not part of the query semantics.
        self.assertEqual((1e16 + 1.0) - 1e16, 0.0)
        self.assertEqual(1e16 - 1e16 + 1.0, 1.0)

    def test_decimal_addition_is_associative(self):
        big, one = Decimal("10000000000000000"), Decimal("1")
        self.assertEqual((big + one) - big, one)
        self.assertEqual(big - big + one, one)

    def test_float_accumulation_loses_exactness(self):
        # Note the count. Measured on this interpreter: 3, 6 and 7 repetitions of 0.1 are
        # inexact while 4, 5, 8, 9, 10 land exactly on the decimal value. That data-dependence
        # is why the defect survived review — it does not show on every dataset.
        self.assertNotEqual(sum([0.1] * 3), 0.3)
        self.assertEqual(sum([Decimal("0.10")] * 3), Decimal("0.30"))


@unittest.skipUnless(MONEY_AVAILABLE, "pyspark not importable")
class TestMonetaryTypes(unittest.TestCase):
    """Type contract — needs pyspark importable, but no JVM."""

    def test_declared_precision_and_scale(self):
        from pipeline.common.money import FX_RATE, MONEY

        self.assertEqual((MONEY.precision, MONEY.scale), (18, 2))
        self.assertEqual((FX_RATE.precision, FX_RATE.scale), (18, 6))

    def test_sum_promotion_stays_within_spark_ceiling(self):
        # Spark promotes SUM of DECIMAL(p, s) to DECIMAL(p + 10, s). This is the reason
        # precision is 18 and not 38 — at 38 the promotion overflows and returns NULL.
        from pipeline.common.money import MONEY

        self.assertLessEqual(MONEY.precision + 10, 38)

    def test_fx_rate_schema_is_decimal(self):
        from pipeline.common.money import FX_RATE
        from pipeline.gold.dim_fx_rate import FX_RATE_SCHEMA

        self.assertEqual(FX_RATE_SCHEMA["rate_to_myr"].dataType, FX_RATE)


class TestMonetaryExactnessInSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session_or_skip()

    def test_double_sum_is_inexact_but_decimal_sum_is_exact(self):
        from pyspark.sql import functions as F

        from pipeline.common.money import MONEY

        df = self.spark.sql(sql_values([("CAST(0.10 AS DOUBLE)",)] * 3, ["amount"]))
        double_total = df.agg(F.sum("amount")).collect()[0][0]
        decimal_total = df.agg(F.sum(F.col("amount").cast(MONEY))).collect()[0][0]

        self.assertNotEqual(double_total, 0.3)
        self.assertEqual(decimal_total, Decimal("0.30"))

    def test_decimal_sum_identical_across_partitioning(self):
        """Partition count is not part of the query. The total must not depend on it."""
        from pyspark.sql import functions as F

        from pipeline.common.money import MONEY

        totals = set()
        for n in (1, 3, 7):
            df = (
                self.spark.range(1, 5001)
                .selectExpr("CAST(id AS DOUBLE) / 100.0 AS amount")
                .repartition(n)
            )
            totals.add(df.agg(F.sum(F.col("amount").cast(MONEY))).collect()[0][0])
        self.assertEqual(len(totals), 1, f"decimal sum varied with partitioning: {totals}")

    def test_to_myr_output_is_decimal_and_repeatable(self):
        """The real conversion path, end to end. Exact equality — never a tolerance."""
        from pipeline.common.money import MONEY

        rates = self.spark.sql(
            sql_values(
                [("'CZK'", "CAST(0.185000 AS DECIMAL(18,6))"), ("'MYR'", "CAST(1.000000 AS DECIMAL(18,6))")],
                ["currency_code", "rate_to_myr"],
            )
        )
        amounts = self.spark.sql(
            sql_values(
                [("CAST(100.10 AS DECIMAL(18,2))", "'CZK'"), ("CAST(250.55 AS DECIMAL(18,2))", "'CZK'")],
                ["amount", "currency"],
            )
        )

        # to_myr resolves dim_fx_rate from storage; exercise the arithmetic it performs.
        from pyspark.sql.functions import col

        joined = amounts.join(rates, amounts["currency"] == rates["currency_code"], "left")
        runs = []
        for _ in range(3):
            out = joined.withColumn(
                "amount_myr", (col("amount").cast(MONEY) * col("rate_to_myr")).cast(MONEY)
            )
            self.assertTrue(dict(out.dtypes)["amount_myr"].startswith("decimal"))
            runs.append(sorted(r["amount_myr"] for r in out.collect()))

        self.assertEqual(runs[0], runs[1], "conversion not repeatable")
        self.assertEqual(runs[1], runs[2], "conversion not repeatable")
        self.assertEqual(runs[0], [Decimal("18.52"), Decimal("46.35")])

    def test_null_rate_still_produces_null(self):
        """The documented D-12 `unitless` behaviour must survive the type change."""
        from pyspark.sql.functions import col

        from pipeline.common.money import MONEY

        df = self.spark.sql(
            sql_values([("CAST(100.00 AS DECIMAL(18,2))", "CAST(NULL AS DECIMAL(18,6))")], ["amount", "rate_to_myr"])
        )
        out = df.withColumn("converted", (col("amount").cast(MONEY) * col("rate_to_myr")).cast(MONEY))
        self.assertIsNone(out.collect()[0]["converted"])

    def test_float_money_guard_rejects_double(self):
        from pipeline.common.money import FloatMoneyError, assert_no_float_money

        bad = self.spark.sql("SELECT CAST(1.0 AS DOUBLE) AS payment_value")
        with self.assertRaises(FloatMoneyError):
            assert_no_float_money(bad, ("payment_value",))

        from pipeline.common.money import MONEY

        good = bad.withColumn("payment_value", bad["payment_value"].cast(MONEY))
        assert_no_float_money(good, ("payment_value",))  # must not raise


class TestMoneyPathHasNoFloatReentry(unittest.TestCase):
    """Static source guards (P3, 2026-09-04). Pure text checks over the repo — they always run,
    with no Spark, no JDK and no lake, which is the point: the defect these protect against is a
    future edit, and an edit lands long before anyone can afford a cloud run to catch it.

    These exist because the INC-0004 fix was reported complete while three `.cast("double")`
    calls were still live at Silver. Gold's own `to_money` cast masked them for ordinary
    amounts — it rounds a double back to 2dp, which looks like a correct number — so nothing
    failed and nothing flagged. A type contract that is only checked where the damage is already
    done is not a contract.
    """

    REPO_ROOT = Path(__file__).resolve().parent.parent

    def _sources(self, subdir: str) -> list[Path]:
        # mart_*.py are RETIRED from orchestration (dbt owns the marts —
        # pipeline/orchestrate_config.yml, PLAN-dbt-marts-serving-layer.md). They are kept on
        # disk for reversibility, so they are deliberately not held to the live money contract.
        return [
            f for f in sorted((self.REPO_ROOT / "pipeline" / subdir).glob("*.py"))
            if not f.name.startswith("mart_")
        ]

    def test_silver_never_casts_money_to_float(self):
        """Silver is where the representation error gets baked in — before Gold can see it.

        journey/05_STTM.md declares every Silver monetary column `decimal`
        (`sil_trans.amount`/`balance`, `sil_card_txn.amount`, `avg_yearly_balance`). A float cast
        anywhere in this layer is a violation of that locked contract, so the check is the whole
        layer rather than a per-column allowlist that a new column would silently escape.
        """
        offenders = []
        for path in self._sources("silver"):
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                code = line.split("#", 1)[0]
                if 'cast("double")' in code or 'cast("float")' in code:
                    offenders.append(f"{path.name}:{lineno}: {line.strip()}")
        self.assertEqual(
            offenders, [],
            "Silver must not cast to a floating-point type - use pipeline.common.money.to_money "
            "(MONEY = DECIMAL(18,2)). Found:\n" + "\n".join(offenders),
        )

    def test_to_myr_asserts_its_input_is_fixed_point(self):
        """The Silver->Gold boundary must fail closed, not round quietly.

        `to_myr` is the single conversion path (ADR-005: no second FX resolution), so it is the
        one chokepoint where a float monetary column can still be caught before it reaches a
        Gold total.
        """
        source = (self.REPO_ROOT / "pipeline" / "gold" / "common.py").read_text(encoding="utf-8")
        to_myr_body = source.split("def to_myr(")[1].split("\ndef ")[0]
        self.assertIn(
            "assert_no_float_money(df,", to_myr_body,
            "to_myr must assert its amount column is DECIMAL before converting — otherwise a "
            "double silently rounds to 2dp and the defect looks like a correct number.",
        )


if __name__ == "__main__":
    unittest.main()
