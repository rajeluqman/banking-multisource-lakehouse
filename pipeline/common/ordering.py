"""Deterministic row-ordering primitives — shared, not duplicated (ADR-007 D7.1).

Three call sites independently picked "one row per key" using an ordering that was not a total
order, so which row survived was decided by partitioning/task scheduling rather than by the
query. Each looked correct — right row count, no error — and each could return a DIFFERENT
survivor on a re-run of the same input:

  - `pipeline/silver/common.py::merge_upsert`            — fell back to `orderBy(lit(1))`
  - `pipeline/silver/common.py::latest_state_from_cdc_log` — `orderBy(...).groupBy(...).first()`
  - `pipeline/gold/common.py::latest_balance_per_account` — `orderBy(date.desc())`, ties per day

That breaks the one property every re-run/backfill/reconciliation argument in this repo depends
on: same input + same code => same output. ADR-009's "never trust run SUCCESS, verify at the
ARTIFACT level" is unenforceable if the artifact is allowed to differ between identical runs.

The fix is one shared helper, not three local patches.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

#: Name of the transient tie-break column. Added, used, and dropped inside this module — it
#: must never reach a Bronze/Silver/Gold artifact.
_TIEBREAK_COL = "_content_tiebreak"


def content_tiebreak(df: DataFrame) -> Column:
    """A deterministic ordering value derived ENTIRELY from the row's own stable content.

    Why a content hash rather than a source column: `merge_upsert` is generic across all five
    domain pipelines and cannot assume any particular ordering column exists — the OBP builders
    curate their `select(...)` down to a few columns and carry no `updated_at`/`created_at` at
    all (see `merge_upsert`'s docstring). A content hash needs no such column.

    Why this is a legitimate tie-break and not merely "a different arbitrary choice":
    two rows that tie on the hash have identical content, so which one survives cannot change
    the output. Ties on the *preferred* ordering columns (e.g. two rows sharing `updated_at`)
    are broken by content, which is stable across runs, partitions and cluster sizes.

    Deliberately NOT used: `rand()`, `uuid()`, `current_timestamp()`,
    `monotonically_increasing_id()`, `input_file_name()`. Every one of them reintroduces the
    defect this module exists to remove.
    """
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns])), 256)


def ordering_columns(available_columns: list[str], preferred: tuple[str, ...] = ("updated_at", "created_at")) -> list[str]:
    """The recency columns to order by, in precedence order, filtered to those that exist.

    Pure list logic, no Spark — so the contract "preferred columns are used when present, and
    their absence is never itself an error" is unit-testable without a SparkSession
    (`tests/test_cdc_ordering_determinism.py`). The caller always appends the content
    tie-break after these; that append is not optional and is not represented here.
    """
    return [c for c in preferred if c in available_columns]


def latest_row_per_key(
    df: DataFrame,
    key_columns: list[str],
    preferred_order: tuple[str, ...] = ("updated_at", "created_at"),
    extra_order: list[Column] | None = None,
) -> DataFrame:
    """Exactly one row per key, chosen deterministically.

    Ordering precedence, most significant first:
      1. `extra_order` — caller-supplied source semantics (e.g. CDC `seq`, Berka `date`)
      2. `preferred_order` columns present on the frame, descending (genuine "latest wins")
      3. `content_tiebreak(df)` — ALWAYS appended, so the ordering is total in effect

    Step 3 is what makes this reproducible. Without it, steps 1-2 leave ties, and `row_number()`
    resolves a tie by whatever order the executor happened to see the rows in.

    Where no recency signal exists at all the survivor is not semantically "the latest" — it is
    a stable, content-determined choice. That limitation is unchanged from the previous
    behaviour and is documented, not hidden; what changes is that it is now the SAME choice on
    every run.
    """
    order: list[Column] = list(extra_order or [])
    order += [F.col(c).desc() for c in ordering_columns(df.columns, preferred_order)]
    order.append(content_tiebreak(df).desc())

    window = Window.partitionBy(*key_columns).orderBy(*order)
    return (
        df.withColumn(_TIEBREAK_COL, F.row_number().over(window))
        .filter(F.col(_TIEBREAK_COL) == 1)
        .drop(_TIEBREAK_COL)
    )
