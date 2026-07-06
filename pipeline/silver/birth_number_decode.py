"""Berka `birth_number` decode (R-12) — YYMMDD where month has +50 added if the client is
female. Pure function, no I/O, so it's unit-testable without Spark/a DB connection
(tests/test_birth_number_decode.py) — this is the one piece of Fasa C logic actually
exercised this session, since it needs no live cloud connection to verify.

D-07: the raw `birth_number` is DROPPED after this decode step — callers must not persist
it past Silver.
"""

from __future__ import annotations

import datetime as dt


class InvalidBirthNumber(ValueError):
    pass


def decode_birth_number(birth_number: str) -> tuple[dt.date, str]:
    """Returns (birth_date, gender). Raises InvalidBirthNumber on a malformed input rather
    than silently guessing — an unparseable value belongs in the R-03-style quarantine
    path, not a best-effort fallback."""
    digits = str(birth_number).strip()
    if len(digits) != 6 or not digits.isdigit():
        raise InvalidBirthNumber(f"expected 6 digits (YYMMDD), got {birth_number!r}")

    yy, mm, dd = int(digits[0:2]), int(digits[2:4]), int(digits[4:6])
    gender = "F" if mm > 50 else "M"
    real_month = mm - 50 if mm > 50 else mm
    if not (1 <= real_month <= 12):
        raise InvalidBirthNumber(f"decoded month {real_month} out of range in {birth_number!r}")

    # Berka is a 1990s Czech dataset — two-digit years are 19xx, not 20xx.
    year = 1900 + yy
    try:
        birth_date = dt.date(year, real_month, dd)
    except ValueError as e:
        raise InvalidBirthNumber(f"invalid calendar date decoded from {birth_number!r}: {e}") from e

    return birth_date, gender
