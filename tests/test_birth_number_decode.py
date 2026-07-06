"""Unit tests for R-12's birth_number decode — known fixtures, per journey/06_DQ_PLAN.md.

Pure-Python, no Spark/DB dependency — run directly:  python -m unittest tests.test_birth_number_decode
"""

from __future__ import annotations

import datetime as dt
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.silver.birth_number_decode import InvalidBirthNumber, decode_birth_number


class TestBirthNumberDecode(unittest.TestCase):
    def test_male(self):
        # 15 Mar 1970, male
        date, gender = decode_birth_number("700315")
        self.assertEqual(date, dt.date(1970, 3, 15))
        self.assertEqual(gender, "M")

    def test_female(self):
        # 15 Mar 1970, female (month + 50 = 53)
        date, gender = decode_birth_number("705315")
        self.assertEqual(date, dt.date(1970, 3, 15))
        self.assertEqual(gender, "F")

    def test_december_female(self):
        date, gender = decode_birth_number("996231")  # month 62 -> real month 12
        self.assertEqual(date, dt.date(1999, 12, 31))
        self.assertEqual(gender, "F")

    def test_wrong_length_raises(self):
        with self.assertRaises(InvalidBirthNumber):
            decode_birth_number("7003")

    def test_non_digit_raises(self):
        with self.assertRaises(InvalidBirthNumber):
            decode_birth_number("70031X")

    def test_invalid_month_raises(self):
        with self.assertRaises(InvalidBirthNumber):
            decode_birth_number("701315")  # month 13, and 63 would be female-Dec, not this

    def test_invalid_calendar_date_raises(self):
        with self.assertRaises(InvalidBirthNumber):
            decode_birth_number("700230")  # Feb 30th doesn't exist


if __name__ == "__main__":
    unittest.main()
