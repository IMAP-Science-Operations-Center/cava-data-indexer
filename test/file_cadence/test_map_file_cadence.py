import unittest
from datetime import datetime, timezone

from data_indexer.file_cadence.map_file_cadence import MapFileCadence, BadFileNameException


class TestMapFileCadence(unittest.TestCase):
    def test_has_correct_name_based_on_parameters(self):
        cases = [
            (MapFileCadence("6mo"), "six_month_map"),
            (MapFileCadence("1yr"), "one_year_map"),
            (MapFileCadence("1mo"), "one_month_map"),
            (MapFileCadence("3mo"), "three_month_map")
        ]

        for cadence, expected_name in cases:
            with self.subTest(cadence.name):
                self.assertEqual(cadence.name, expected_name)

    def test_get_file_time_range(self):
        cases = [
            (MapFileCadence("6mo"), datetime(2025, 7, 1, tzinfo=timezone.utc), datetime(2025, 12, 30, 15, tzinfo=timezone.utc)),
            (MapFileCadence("1mo"), datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2025, 1, 31, 10, 30, tzinfo=timezone.utc)),
            (MapFileCadence("3mo"), datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2025, 4, 2, 7, 30, tzinfo=timezone.utc)),
            (MapFileCadence("1yr"), datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, 6, tzinfo=timezone.utc)),
        ]

        for cadence, start_time, expected_end_time in cases:
            with self.subTest(f"{cadence}, {start_time}"):
                actual_start_time, actual_end_time = cadence.get_file_time_range(start_time)

                self.assertEqual(start_time, actual_start_time)
                self.assertEqual(expected_end_time, actual_end_time)

    def test_throws_exception_on_bad_cadence(self):
        with self.assertRaises(BadFileNameException) as context:
            MapFileCadence("bad")
        self.assertIn("Cannot parse map with cadence: bad", str(context.exception))
