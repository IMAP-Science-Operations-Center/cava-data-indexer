import unittest
from datetime import date, datetime, timezone

from data_indexer import utils
from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalInfo
from data_indexer.cdf_parser.cdf_parser import CdfFileInfo
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableInfo
from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence
from data_indexer.utils import DataProductSource


class TestUtils(unittest.TestCase):
    def test_get_index(self):
        cdf_file_info = CdfFileInfo(CdfGlobalInfo("source", "source in human", "123", date(2022, 7, 28)),
                                    [CdfVariableInfo("cdf_var_1", "Variable 1", "spectrogram", "units")])
        data_product_source_1 = DataProductSource("url_1",
                                                  datetime(2025, 1, 1, tzinfo=timezone.utc),
                                                  datetime(2025, 1, 2, tzinfo=timezone.utc))
        data_product_source_2 = DataProductSource("url_2",
                                                  datetime(2025, 1, 2, tzinfo=timezone.utc),
                                                  datetime(2025, 1, 3, tzinfo=timezone.utc))

        output = utils.get_index_entry(
            cdf_file_info=cdf_file_info,
            instrument="isois",
            mission="PSP",
            file_cadence=DailyFileCadence(),
            file_timeranges=[data_product_source_1, data_product_source_2]
        )
        expected = {
            'file_timeranges': [
                {
                    'start_time': '2025-01-01T00:00:00+00:00',
                    'end_time': '2025-01-02T00:00:00+00:00',
                    'url': 'url_1'
                },
                {
                    'start_time': '2025-01-02T00:00:00+00:00',
                    'end_time': '2025-01-03T00:00:00+00:00',
                    'url': 'url_2'
                }
            ],
            'variables': [{'catalog_description': 'Variable 1', 'variable_name': 'cdf_var_1',
                           'display_type': 'spectrogram', 'units': 'units'}],
            'logical_source': 'source',
            'logical_source_description': 'source in human',
            'generation_date': '2022-07-28',
            'instrument': 'isois',
            'mission': 'PSP', 'file_cadence': 'daily'
        }
        self.assertEqual(expected, output)


if __name__ == '__main__':
    unittest.main()
