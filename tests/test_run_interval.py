"""Unit tests for pipeline/common/run_interval.py — the ADR-011 D11.5 helper
(PIPELINE_SIDE_CONTRACT.md §3: `data_interval_start` maps to `dt`, never `now()`).

Pure-Python, no Spark/DB dependency — run directly:
  python -m unittest tests.test_run_interval
"""

from __future__ import annotations

import datetime as dt
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.common.run_interval import interval_window, logical_date


class TestLogicalDate(unittest.TestCase):
    def test_env_set_uses_interval_start_date(self):
        with mock.patch.dict("os.environ", {"DATA_INTERVAL_START": "2026-07-01T00:00:00+00:00"}, clear=False):
            self.assertEqual(logical_date(), "2026-07-01")

    def test_env_unset_falls_back_to_today(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(logical_date(), dt.date.today().isoformat())

    def test_backfill_logical_date_is_not_todays_date(self):
        # The concrete failure mode from the contract violation: a backfill for a past
        # logical date must not resolve to wall-clock today.
        with mock.patch.dict("os.environ", {"DATA_INTERVAL_START": "2020-01-15T00:00:00+00:00"}, clear=False):
            resolved = logical_date()
        self.assertEqual(resolved, "2020-01-15")
        self.assertNotEqual(resolved, dt.date.today().isoformat())


class TestIntervalWindow(unittest.TestCase):
    def test_both_set_returns_bounded_window(self):
        env = {
            "DATA_INTERVAL_START": "2026-07-01T00:00:00+00:00",
            "DATA_INTERVAL_END": "2026-07-02T00:00:00+00:00",
        }
        with mock.patch.dict("os.environ", env, clear=False):
            window = interval_window(dt.timedelta(minutes=5))
        self.assertIsNotNone(window)
        start, end = window
        self.assertEqual(start, dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc) - dt.timedelta(minutes=5))
        self.assertEqual(end, dt.datetime(2026, 7, 2, tzinfo=dt.timezone.utc))

    def test_only_start_set_returns_none(self):
        with mock.patch.dict("os.environ", {"DATA_INTERVAL_START": "2026-07-01T00:00:00+00:00"}, clear=True):
            self.assertIsNone(interval_window(dt.timedelta(minutes=5)))

    def test_neither_set_returns_none(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertIsNone(interval_window(dt.timedelta(minutes=5)))


if __name__ == "__main__":
    unittest.main()
