from datetime import datetime
from unittest import TestCase

from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence


class TestDailyFileCadence(TestCase):

    def test_get_file_time_range(self):
        cases = [
            (datetime(2023, 12, 1, 0, 0, 0), (datetime(2023, 12, 1), datetime(2023, 12, 2))),
            (datetime(2023, 12, 1, 12, 0, 0), (datetime(2023, 12, 1), datetime(2023, 12, 2)))
        ]
        for time, expected_range in cases:
            with self.subTest(time):
                self.assertEqual(expected_range, DailyFileCadence().get_file_time_range(time))
