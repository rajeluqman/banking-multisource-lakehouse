"""Regression tests for INC-0003 — one-row-per-key selection must be reproducible.

Covers all three call sites the incident found:
  - `pipeline/silver/common.py::merge_upsert`              (was `orderBy(lit(1))`)
  - `pipeline/silver/common.py::latest_state_from_cdc_log` (was `groupBy(...).first()`)
  - `pipeline/gold/common.py::latest_balance_per_account`  (was `orderBy(date.desc())`, day ties)

The failing property is not "wrong row count" — the count was always right, which is why this
survived review. It is that the SAME input could yield a DIFFERENT survivor on a re-run.

Run:  python -m unittest tests.test_cdc_ordering_determinism -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests._spark_support import MONEY_AVAILABLE, spark_session_or_skip, sql_values


@unittest.skipUnless(MONEY_AVAILABLE, "pyspark not importable")
class TestOrderingColumnSelection(unittest.TestCase):
    """Pure list logic — needs pyspark importable but no JVM."""

    def test_prefers_updated_at_then_created_at(self):
        from pipeline.common.ordering import ordering_columns

        self.assertEqual(ordering_columns(["id", "updated_at", "created_at"]), ["updated_at", "created_at"])

    def test_absent_recency_columns_are_not_an_error(self):
        """The OBP builders carry neither column. That must not raise, and must not disable
        dedup — Delta's one-row-per-match-key requirement does not care."""
        from pipeline.common.ordering import ordering_columns

        self.assertEqual(ordering_columns(["account_id", "balance"]), [])


class TestDeterministicSurvivor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session_or_skip()

    def _tied_frame(self):
        """Two rows for the same PK that tie on every recency column, differing only in payload.
        This is the exact shape that made the old `lit(1)` fallback non-deterministic."""
        return self.spark.sql(
            sql_values(
                [
                    ("'C1'", "'2026-09-01'", "'ALICE-A'"),
                    ("'C1'", "'2026-09-01'", "'ALICE-B'"),
                    ("'C2'", "'2026-09-01'", "'BOB'"),
                ],
                ["pk", "updated_at", "name"],
            )
        )

    def test_survivor_is_stable_across_repeated_runs(self):
        from pipeline.common.ordering import latest_row_per_key

        survivors = set()
        for _ in range(10):
            out = latest_row_per_key(self._tied_frame(), ["pk"])
            survivors.add(tuple(sorted((r["pk"], r["name"]) for r in out.collect())))
        self.assertEqual(len(survivors), 1, f"survivor changed between runs: {survivors}")

    def test_exactly_one_row_per_key(self):
        from pipeline.common.ordering import latest_row_per_key

        out = latest_row_per_key(self._tied_frame(), ["pk"])
        self.assertEqual(out.count(), 2)
        self.assertEqual(out.select("pk").distinct().count(), 2)

    def test_survivor_is_stable_across_partitioning(self):
        """Partition count is not part of the query semantics."""
        from pipeline.common.ordering import latest_row_per_key

        results = []
        for n in (1, 3, 7):
            out = latest_row_per_key(self._tied_frame().repartition(n), ["pk"])
            results.append(sorted((r["pk"], r["name"]) for r in out.collect()))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])

    def test_recency_column_still_wins_when_present(self):
        """The fix must not cost the real semantics: a genuine `updated_at` still decides."""
        from pipeline.common.ordering import latest_row_per_key

        df = self.spark.sql(
            sql_values(
                [("'C1'", "'2026-01-01'", "'OLD'"), ("'C1'", "'2026-09-01'", "'NEW'")],
                ["pk", "updated_at", "name"],
            )
        )
        out = latest_row_per_key(df, ["pk"]).collect()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["name"], "NEW")

    def test_extra_order_takes_precedence(self):
        """CDC `seq` and Berka `date` are passed as extra_order and must outrank everything."""
        from pyspark.sql.functions import col

        from pipeline.common.ordering import latest_row_per_key

        df = self.spark.sql(
            sql_values(
                [("'K1'", "1", "'FIRST'"), ("'K1'", "9", "'LATEST'"), ("'K1'", "5", "'MIDDLE'")],
                ["pk_value", "seq", "op"],
            )
        )
        out = latest_row_per_key(df, ["pk_value"], extra_order=[col("seq").desc()]).collect()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["op"], "LATEST")

    def test_no_tiebreak_column_leaks_into_output(self):
        """The transient ordering column must never reach a Silver/Gold artifact."""
        from pipeline.common.ordering import latest_row_per_key

        out = latest_row_per_key(self._tied_frame(), ["pk"])
        self.assertEqual(set(out.columns), {"pk", "updated_at", "name"})

    def test_identical_rows_collapse_regardless_of_survivor(self):
        """Rows tying on the content hash are identical, so the choice cannot change output."""
        from pipeline.common.ordering import latest_row_per_key

        df = self.spark.sql(
            sql_values([("'D1'", "'2026-09-01'", "'SAME'"), ("'D1'", "'2026-09-01'", "'SAME'")], ["pk", "updated_at", "name"])
        )
        out = latest_row_per_key(df, ["pk"]).collect()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["name"], "SAME")


if __name__ == "__main__":
    unittest.main()
