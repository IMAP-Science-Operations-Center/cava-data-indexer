import unittest
from datetime import date

from data_indexer import utils
from data_indexer.cdf_downloader.psp_downloader import FileCadence
from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalInfo
from data_indexer.cdf_parser.cdf_parser import CdfFileInfo
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableInfo


class TestUtils(unittest.TestCase):
    def test_get_index(self):
        cdf_file_info = CdfFileInfo(CdfGlobalInfo("source", "source in human", "123", date(2022,7,28)),
                                    [CdfVariableInfo("cdf_var_1","Variable 1", "spectrogram")])
        output = utils.get_index_entry(
            cdf_file_info,
            "source_url_%yyyymmdd%.cdf",
            "link_to_example_file.cdf",
            [[date(1900, 1, 1), date(2022, 12, 1)],
             [date(2022, 12, 3), date(2045, 6, 7)]],
            "isois",
            "PSP",
            FileCadence.DAILY)
        expected = {'dates_available': [['1900-01-01', '2022-12-01'], ['2022-12-03', '2045-06-07']],
                    'description_source_file': 'link_to_example_file.cdf',
                    'variables': [{'catalog_description':'Variable 1','variable_name': 'cdf_var_1','display_type':'spectrogram'}],
                    'logical_source': 'source',
                    'logical_source_description': 'source in human',
                    'source_file_format': 'source_url_%yyyymmdd%.cdf',
                    'version': '123',
                    'generation_date': '2022-07-28',
                    'instrument': 'isois',
                    'mission': 'PSP', 'file_cadence':'daily'}
        self.assertEqual(expected, output)


if __name__ == '__main__':
    unittest.main()
