"""Shared seeding utilities implementing D-03 (seeding rules, locked).

Every source loader imports from here rather than reimplementing date-rebase,
PK/timestamp assignment, or the deterministic RNG — D-03.4 requires a rebuild
from scratch to produce identical databases, so the seed must be fixed in one
place, not per-loader.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import random

SEED = 20260705  # fixed seed (D-03.4) — do not change without a documented reason
SEED_DAY = dt.date.today()  # max(date) target for the D-03.2 global date-rebase


def seeded_random(namespace: str) -> random.Random:
    """A Random instance derived from the fixed SEED + a namespace string, so each
    loader gets an independent-looking but fully reproducible stream (D-03.4)."""
    digest = hashlib.sha256(f"{SEED}:{namespace}".encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


def rebase_dates(dates: list[dt.date], target_max: dt.date = SEED_DAY) -> list[dt.date]:
    """D-03.2 global date-rebase: shift a whole date series so max(date) = target_max,
    preserving relative offsets between all dates in the series."""
    if not dates:
        return []
    offset = target_max - max(dates)
    return [d + offset for d in dates]


def paysim_step_to_timestamp(step: int, base_date: dt.date) -> dt.datetime:
    """D-03.3: PaySim's `step` is a simulation-hour index (1-744) -> real timestamp."""
    return dt.datetime.combine(base_date, dt.time.min) + dt.timedelta(hours=step)


def with_pk_and_timestamps(row: dict, pk_column: str, pk_value, created_at: dt.datetime | None = None) -> dict:
    """D-03.1: every seeded table gets a PK + created_at/updated_at."""
    ts = created_at or dt.datetime.combine(SEED_DAY, dt.time.min)
    return {pk_column: pk_value, **row, "created_at": ts, "updated_at": ts, "is_deleted": False}


def currency_tag(row: dict, currency_column: str, code: str) -> dict:
    """D-12: every monetary column gets a currency code at seed."""
    return {**row, currency_column: code}
