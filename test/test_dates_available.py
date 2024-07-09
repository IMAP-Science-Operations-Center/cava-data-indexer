import unittest
from datetime import date, datetime

from data_indexer import dates_available


class TestDatesAvailable(unittest.TestCase):
    def test_single_day(self):
        dates = [date(2020, 1, 1)]

        output = dates_available.get_date_ranges(dates)
        expected_output = [[date(2020, 1, 1), date(2020, 1, 1)]]

        self.assertEqual(expected_output, output)

    def test_contiguous_range(self):
        dates = [date(2008, 1, 31), date(2008, 2, 1), date(2008, 2, 2), date(2008, 2, 3)]

        output = dates_available.get_date_ranges(dates)
        expected_output = [[date(2008, 1, 31), date(2008, 2, 3)]]

        self.assertEqual(expected_output, output)

    def test_multiple_ranges(self):
        dates = [date(2008, 1, 2), date(2008, 1, 3), date(2008, 1, 5), date(2008, 1, 6), date(2008, 1, 7),
                 date(2008, 2, 1)]

        output = dates_available.get_date_ranges(dates)
        expected_output = [[date(2008, 1, 2), date(2008, 1, 3)], [date(2008, 1, 5), date(2008, 1, 7)],
                           [date(2008, 2, 1), date(2008, 2, 1)]]

        self.assertEqual(expected_output, output)


    def test_contiguous_range_from_ranges(self):
        cases = [('with no gap',
                  [(datetime(2008, 1, 31), datetime(2008, 2, 1)), (datetime(2008, 2, 1), datetime(2008, 2, 20))],
                  [(datetime(2008, 1, 31), datetime(2008, 2, 20))]),
                 ('with gap',
                  [(datetime(2008, 1, 31), datetime(2008, 2, 1)), (datetime(2008, 2, 3), datetime(2008, 2, 20))],
                  [(datetime(2008, 1, 31), datetime(2008, 2, 1)), (datetime(2008, 2, 3), datetime(2008, 2, 20))]),

                 ('unsorted',
                  [(datetime(2008, 2, 3), datetime(2008, 2, 20)), (datetime(2008, 1, 31), datetime(2008, 2, 1))],
                  [(datetime(2008, 1, 31), datetime(2008, 2, 1)), (datetime(2008, 2, 3), datetime(2008, 2, 20))]),
                 ('overlap',
                  [(datetime(2008, 1, 31), datetime(2008, 2, 3)), (datetime(2008, 2, 1), datetime(2008, 2, 5))],
                  [(datetime(2008, 1, 31), datetime(2008, 2, 5))])
                 ]
        for name, input, expected_output in cases:
            with self.subTest(name):
                output = dates_available.get_contiguous_ranges(input)
                self.assertEqual(expected_output, output)

if __name__ == '__main__':
    unittest.main()
