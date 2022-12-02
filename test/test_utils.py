import unittest
from datetime import date

from src import utils
from src.cdf_parser.cdf_global_parser import CdfGlobalInfo
from src.cdf_parser.cdf_parser import CdfFileInfo


class TestUtils(unittest.TestCase):
    def test_get_index(self):
        cdf_file_info = CdfFileInfo(CdfGlobalInfo("source", "123"), {"Variable 1": "cdf_var_1"})
        output = utils.get_index_entry(
            cdf_file_info,
            "source_url_%yyyymmdd%.cdf",
            "link_to_example_file.cdf",
            [[date(1900, 1, 1), date(2022, 12, 1)],
             [date(2022,12,3), date(2045,6,7)]])
        expected = {'dates_available': [['1900-01-01', '2022-12-01'],['2022-12-03','2045-06-07']],
                    'description_source_file': 'link_to_example_file.cdf',
                    'descriptions': {'Variable 1': 'cdf_var_1'},
                    'logical_source': 'source',
                    'source_file_format': 'source_url_%yyyymmdd%.cdf',
                    'version': '123'}
        self.assertEqual(expected, output)


if __name__ == '__main__':
    unittest.main()
