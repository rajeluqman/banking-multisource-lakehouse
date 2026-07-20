"""Regression tests for the ADR-011 D11.5 contract-compliance fix (staff-DE ruling,
2026-07-20): every Landing extractor must key `dt=` off `DATA_INTERVAL_START` when Airflow
sets it, never off wall-clock `date.today()`; the two time-watermarked extractors
(jdbc_batch_common, salesforce_extract) must additionally bound their pull to
`[DATA_INTERVAL_START - overlap, DATA_INTERVAL_END)` and skip lake-watermark read/write in
that mode. No live DB/Spark/Salesforce connection required — all I/O is faked.

Run directly:  python -m unittest tests.test_extract_logical_date
"""

from __future__ import annotations

import datetime as dt
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from pipeline.extract import cdc_common, cdc_initial_snapshot, obp_client, salesforce_extract
from pipeline.extract import jdbc_batch_common

INTERVAL_ENV = {
    "DATA_INTERVAL_START": "2020-01-15T00:00:00+00:00",
    "DATA_INTERVAL_END": "2020-01-16T00:00:00+00:00",
}


class _FakeReadBuilder:
    """Stands in for spark.read.format(...).option(...).options(...).load() — captures every
    `.option()`/`.options()` call so the test can inspect the JDBC predicate, without needing
    a real Spark session or JDBC connection."""

    def __init__(self, df):
        self._df = df
        self.captured: dict = {}

    def format(self, _fmt):
        return self

    def option(self, key, value):
        self.captured[key] = value
        return self

    def options(self, **kwargs):
        self.captured.update(kwargs)
        return self

    def load(self):
        return self._df


class _FakeDF:
    def __init__(self, row_count=0):
        self._row_count = row_count
        self.write = self

    def mode(self, _):
        return self

    def parquet(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)

    def count(self):
        return self._row_count


class _FakeSpark:
    def __init__(self, reader: _FakeReadBuilder):
        self.read = reader


class _FakeBulk2Object:
    def __init__(self, pages: list[str], captured_soql: list[str]):
        self._pages = pages
        self._captured = captured_soql

    def query(self, soql: str):
        self._captured.append(soql)
        return self._pages


class _FakeBulk2:
    def __init__(self, pages: list[str], captured_soql: list[str]):
        self._pages = pages
        self._captured = captured_soql

    def __getattr__(self, _name):
        return _FakeBulk2Object(self._pages, self._captured)


class _FakeSF:
    def __init__(self, pages: list[str], captured_soql: list[str]):
        self.bulk2 = _FakeBulk2(pages, captured_soql)


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _sql, _params):
        pass

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)


class ExtractDtPartitioningTestCase(unittest.TestCase):
    """Common tmp-lake harness — patches `lake_root()` once so every module's `layer_path()`
    call (all of them import it from `pipeline.common.lake_paths`) resolves under a scratch
    directory instead of the real repo `data/` fallback."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        patcher = mock.patch("pipeline.common.lake_paths.lake_root", return_value=self._tmpdir.name)
        patcher.start()
        self.addCleanup(patcher.stop)


class TestJdbcBatchCommon(ExtractDtPartitioningTestCase):
    def test_interval_mode_dt_and_bounded_predicate_no_watermark_io(self):
        reader = _FakeReadBuilder(_FakeDF(row_count=0))
        spark = _FakeSpark(reader)
        with mock.patch.dict("os.environ", INTERVAL_ENV, clear=False), \
             mock.patch.object(jdbc_batch_common, "read_watermark") as rw, \
             mock.patch.object(jdbc_batch_common, "write_watermark") as ww:
            path = jdbc_batch_common.extract_table(spark, "postgres", "application", "jdbc:fake", {})

        self.assertIn("dt=2020-01-15", path)
        predicate = reader.captured["dbtable"]
        self.assertIn("updated_at > '2020-01-14T23:55:00", predicate)  # start - 5min overlap
        self.assertIn("updated_at < '2020-01-16T00:00:00", predicate)
        rw.assert_not_called()
        ww.assert_not_called()

    def test_fallback_mode_uses_watermark_and_todays_date(self):
        reader = _FakeReadBuilder(_FakeDF(row_count=0))
        spark = _FakeSpark(reader)
        with mock.patch.dict("os.environ", {}, clear=True), \
             mock.patch.object(jdbc_batch_common, "read_watermark", return_value=None) as rw, \
             mock.patch.object(jdbc_batch_common, "write_watermark") as ww:
            path = jdbc_batch_common.extract_table(spark, "postgres", "application", "jdbc:fake", {})

        self.assertIn(f"dt={dt.date.today().isoformat()}", path)
        self.assertEqual(reader.captured["dbtable"], "(SELECT * FROM application WHERE 1=1) AS t")
        rw.assert_called_once()
        ww.assert_called_once()

    def test_backfill_is_deterministic_across_repeated_calls(self):
        reader1 = _FakeReadBuilder(_FakeDF(row_count=0))
        reader2 = _FakeReadBuilder(_FakeDF(row_count=0))
        with mock.patch.dict("os.environ", INTERVAL_ENV, clear=False), \
             mock.patch.object(jdbc_batch_common, "read_watermark"), \
             mock.patch.object(jdbc_batch_common, "write_watermark"):
            path1 = jdbc_batch_common.extract_table(_FakeSpark(reader1), "postgres", "application", "jdbc:fake", {})
            path2 = jdbc_batch_common.extract_table(_FakeSpark(reader2), "postgres", "application", "jdbc:fake", {})

        self.assertEqual(path1, path2)
        self.assertEqual(reader1.captured["dbtable"], reader2.captured["dbtable"])


class TestSalesforceExtract(ExtractDtPartitioningTestCase):
    CSV_PAGE = (
        "Id,berka_client_id__c,birth_number__c,berka_district_id__c,SystemModstamp\n"
        "001,c1,700101,d1,2020-01-15T12:00:00.000+0000\n"
    )

    def test_interval_mode_dt_and_bounded_soql_no_watermark_io(self):
        captured_soql: list[str] = []
        sf = _FakeSF([self.CSV_PAGE], captured_soql)
        with mock.patch.dict("os.environ", INTERVAL_ENV, clear=False), \
             mock.patch.object(salesforce_extract, "read_watermark") as rw, \
             mock.patch.object(salesforce_extract, "write_watermark") as ww:
            path = salesforce_extract.extract_object(sf, "salesforce", "contact")

        self.assertIn("dt=2020-01-15", path)
        self.assertEqual(len(captured_soql), 1)
        self.assertIn("SystemModstamp > 2020-01-14T23:55:00", captured_soql[0])
        self.assertIn("SystemModstamp < 2020-01-16T00:00:00", captured_soql[0])
        rw.assert_not_called()
        ww.assert_not_called()

    def test_fallback_mode_uses_watermark_and_todays_date(self):
        captured_soql: list[str] = []
        sf = _FakeSF([self.CSV_PAGE], captured_soql)
        with mock.patch.dict("os.environ", {}, clear=True), \
             mock.patch.object(salesforce_extract, "read_watermark", return_value=None) as rw, \
             mock.patch.object(salesforce_extract, "write_watermark") as ww:
            path = salesforce_extract.extract_object(sf, "salesforce", "contact")

        self.assertIn(f"dt={dt.date.today().isoformat()}", path)
        self.assertNotIn("WHERE", captured_soql[0])  # first run, no watermark yet -> no predicate
        rw.assert_called_once()
        ww.assert_called_once()


class TestCdcCommon(ExtractDtPartitioningTestCase):
    ROWS = [(1, "I", "pk1", "2020-01-15T12:00:00")]

    def test_dt_follows_logical_date_seq_offset_unaffected(self):
        conn = _FakeConnection(self.ROWS)
        with mock.patch.dict("os.environ", INTERVAL_ENV, clear=False), \
             mock.patch.object(cdc_common, "read_watermark", return_value=None), \
             mock.patch.object(cdc_common, "write_watermark") as ww:
            path = cdc_common.poll_cdc_log(conn, "teradata", "bank_marketing")

        self.assertIsNotNone(path)
        self.assertIn("dt=2020-01-15", path)
        ww.assert_called_once_with("teradata", "bank_marketing_cdc_log", "1")  # seq offset, not interval-derived

    def test_dt_falls_back_to_today_without_interval_env(self):
        conn = _FakeConnection(self.ROWS)
        with mock.patch.dict("os.environ", {}, clear=True), \
             mock.patch.object(cdc_common, "read_watermark", return_value=None), \
             mock.patch.object(cdc_common, "write_watermark"):
            path = cdc_common.poll_cdc_log(conn, "teradata", "bank_marketing")

        self.assertIn(f"dt={dt.date.today().isoformat()}", path)


class TestObpClient(ExtractDtPartitioningTestCase):
    def test_dt_follows_logical_date(self):
        with mock.patch.dict("os.environ", INTERVAL_ENV, clear=False):
            path = obp_client._land([{"id": "a1"}], "accounts")
        self.assertIn("dt=2020-01-15", path)

    def test_dt_falls_back_to_today_without_interval_env(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            path = obp_client._land([{"id": "a1"}], "accounts")
        self.assertIn(f"dt={dt.date.today().isoformat()}", path)


class TestCdcInitialSnapshot(ExtractDtPartitioningTestCase):
    def test_dt_follows_logical_date(self):
        df = pd.DataFrame([{"id": 1}])
        with mock.patch.dict("os.environ", INTERVAL_ENV, clear=False), \
             mock.patch.object(cdc_initial_snapshot, "read_watermark", return_value=None), \
             mock.patch.object(cdc_initial_snapshot, "write_watermark"):
            path = cdc_initial_snapshot.extract_initial_snapshot(df, "teradata", "bank_marketing")

        self.assertIn("dt=2020-01-15", path)

    def test_dt_falls_back_to_today_without_interval_env(self):
        df = pd.DataFrame([{"id": 1}])
        with mock.patch.dict("os.environ", {}, clear=True), \
             mock.patch.object(cdc_initial_snapshot, "read_watermark", return_value=None), \
             mock.patch.object(cdc_initial_snapshot, "write_watermark"):
            path = cdc_initial_snapshot.extract_initial_snapshot(df, "teradata", "bank_marketing")

        self.assertIn(f"dt={dt.date.today().isoformat()}", path)


if __name__ == "__main__":
    unittest.main()
