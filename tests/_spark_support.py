"""Shared test helpers for the Spark-dependent regression tests.

Kept out of the test modules so both can skip identically, and so CI (which runs
`python -m unittest discover -s tests` and may have neither pyspark nor a JDK) degrades to the
pure-Python assertions rather than erroring.

Leading underscore so `unittest discover` does not collect this file as a test module.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:  # importable without a JVM — only creating a session needs Java
    import pyspark  # noqa: F401

    MONEY_AVAILABLE = True
except Exception:  # pragma: no cover - environment dependent
    MONEY_AVAILABLE = False

_SESSION = None
_SESSION_ERROR: str | None = None


def spark_session_or_skip():
    """A local SparkSession, or `skipTest` with the reason.

    Pins `PYSPARK_PYTHON` to the running interpreter and the bind address to loopback. Both are
    needed on Windows, where the default hostname can resolve to a Docker interface and the
    worker launch then fails with an unrelated-looking handshake error.
    """
    global _SESSION, _SESSION_ERROR

    if not MONEY_AVAILABLE:
        raise unittest.SkipTest("pyspark not installed")
    if _SESSION is not None:
        return _SESSION
    if _SESSION_ERROR is not None:
        raise unittest.SkipTest(_SESSION_ERROR)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    try:
        from pyspark.sql import SparkSession

        _SESSION = (
            SparkSession.builder.appName("banking-determinism-tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        _SESSION.sparkContext.setLogLevel("ERROR")
        return _SESSION
    except Exception as exc:  # pragma: no cover - environment dependent
        _SESSION_ERROR = f"SparkSession unavailable ({type(exc).__name__}: {exc})"
        raise unittest.SkipTest(_SESSION_ERROR)


def sql_values(rows: list[tuple[str, ...]], columns: list[str]) -> str:
    """Build a `SELECT * FROM VALUES ...` fixture.

    Used instead of `spark.createDataFrame(python_list, ...)` on purpose: that path parallelises
    a pickled Python list and therefore needs a Python worker, which fails on
    PySpark 4.x + CPython 3.13 on Windows. SQL literals are materialised entirely JVM-side, so
    the same fixture runs on the repo's pinned pyspark 3.5.3 and on a newer local install.
    Any Spark test added here must follow the same rule.
    """
    tuples = ", ".join("(" + ", ".join(r) + ")" for r in rows)
    return f"SELECT * FROM VALUES {tuples} AS t({', '.join(columns)})"
