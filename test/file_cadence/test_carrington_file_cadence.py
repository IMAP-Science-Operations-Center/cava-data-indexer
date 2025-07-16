import unittest
from datetime import datetime, timezone, timedelta

from data_indexer.file_cadence.carrington_file_cadence import CarringtonFileCadence


class TestCarringtonFileCadence(unittest.TestCase):
    def test_get_file_time_range_with_start_date(self):
        cases = [
            (datetime(1853, 10, 13, tzinfo=timezone.utc), datetime(1853, 10, 13, 14, tzinfo=timezone.utc), datetime(1853, 11, 9, 21, tzinfo=timezone.utc)),
            (datetime(2025, 6, 19, tzinfo=timezone.utc), datetime(2025, 6, 19, 12, tzinfo=timezone.utc), datetime(2025, 7, 16, 17, tzinfo=timezone.utc))
        ]

        for start_date, expected_start, expected_end in cases:
            with self.subTest(start_date):
                actual_start, actual_end = CarringtonFileCadence.get_file_time_range(start_date)

                difference_between_starts = actual_start - expected_start
                self.assertLess(abs(difference_between_starts), timedelta(hours=2))

                difference_between_ends = actual_end - expected_end
                self.assertLess(abs(difference_between_ends), timedelta(hours=2))

    def test_get_file_time_range_with_cr(self):
        cases = [
            (0, datetime(1853, 10, 13, 14, tzinfo=timezone.utc), datetime(1853, 11, 9, 21, tzinfo=timezone.utc)),
            (2299, datetime(2025, 6, 19, 12, tzinfo=timezone.utc), datetime(2025, 7, 16, 17, tzinfo=timezone.utc))
        ]

        for cr, expected_start, expected_end in cases:
            with self.subTest(cr):
                actual_start, actual_end = CarringtonFileCadence.get_file_time_range_with_cr(cr)

                difference_between_starts = actual_start - expected_start
                self.assertLess(abs(difference_between_starts), timedelta(hours=2))

                difference_between_ends = actual_end - expected_end
                self.assertLess(abs(difference_between_ends), timedelta(hours=2))
