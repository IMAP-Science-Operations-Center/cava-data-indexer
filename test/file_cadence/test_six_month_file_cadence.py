from datetime import datetime
from unittest import TestCase


from data_indexer.file_cadence.six_month_file_cadence import SixMonthFileCadence


class TestSixMonthFileCadence(TestCase):

    def test_get_file_time_range(self):
        cases = [
            (datetime(2023, 1, 1, 0, 0, 0), (datetime(2023, 1, 1), datetime(2023, 7, 1))),
            (datetime(2023, 6, 30, 23, 59, 59, 999999), (datetime(2023, 1, 1), datetime(2023, 7, 1))),
            (datetime(2020, 7, 1, 0, 0, 0), (datetime(2020, 7, 1), datetime(2021, 1, 1))),
            (datetime(2020, 12, 31, 23, 59, 59, 999999), (datetime(2020, 7, 1), datetime(2021, 1, 1))),
        ]
        for time, expected_range in cases:
            with self.subTest(time):
                self.assertEqual(expected_range, SixMonthFileCadence.get_file_time_range(time))
