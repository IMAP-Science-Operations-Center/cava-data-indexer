import unittest
from datetime import date
from unittest.mock import patch, call, sentinel, Mock

from data_indexer.cdf_downloader.psp_downloader import PspDirectoryInfo, psp_isois_cda_base_url
from data_indexer.cdf_downloader.psp_file_parser import PspFileInfo
from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalInfo
from data_indexer.cdf_parser.cdf_parser import CdfFileInfo
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableInfo
from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence
from data_indexer.file_cadence.six_month_file_cadence import SixMonthFileCadence
from data_indexer.psp_data_processor import PspDataProcessor


class TestPspDataProcessor(unittest.TestCase):

    @patch('data_indexer.psp_data_processor.CdfParser')
    @patch('data_indexer.psp_data_processor.PspDownloader')
    @patch('data_indexer.psp_data_processor.get_with_retry')
    def test_gets_filenames_and_downloads_the_first_file_in_list(self, mock_get_with_retry, mock_downloader,
                                                                 mock_cdf_parser):
        self.maxDiff = None
        mock_downloader.get_all_metadata.return_value = \
            [PspDirectoryInfo(psp_isois_cda_base_url, 'ISOIS-EPIHi', 'epihi', {
                'het_rate1': [PspFileInfo('link1', 'psp_isois-epihi_l2-het-rates3600_20190102_v9.cdf', '2022'),
                              PspFileInfo('link1', 'psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf', '2022'),
                              PspFileInfo('link2', 'psp_isois-epihi_l2-het-rates3600_20190103_v10.cdf', '2022')],
                'het_rate2': [PspFileInfo('link3', 'psp_isois-epihi_l2-het-rates60_20190102_v11.cdf', '2023'),
                              PspFileInfo('link4', 'psp_isois-epihi_l2-het-rates60_20190105_v11.cdf', '2023')]},
                              sentinel.variable_selector_1, 'PSP', DailyFileCadence()),
             PspDirectoryInfo(psp_isois_cda_base_url, 'ISOIS', 'merged',
                              {
                                  'ephem': [
                                      PspFileInfo('linkfile_to_download5.cdf', 'psp_isois_l2-ephem_20181111_v12.cdf',
                                                  '2022'),
                                      PspFileInfo('linkfile_to_download6.cdf', 'psp_isois_l2-ephem_20181112_v12.cdf',
                                                  '2022')],
                                  'summary': [
                                      PspFileInfo('linkfile_to_download7.cdf', 'psp_isois_l2-summary_20181114_v13.cdf',
                                                  '2023'),
                                      PspFileInfo('linkfile_to_download8.cdf', 'psp_isois_l2-summary_20181115_v13.cdf',
                                                  '2023')]},
                              sentinel.variable_selector_2, 'Not PSP', SixMonthFileCadence())
             ]

        het_rates3600_20190102_url = psp_isois_cda_base_url + '/2022/psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf'
        het_rates3600_20190103_url = psp_isois_cda_base_url + '/2022/psp_isois-epihi_l2-het-rates3600_20190103_v10.cdf'
        het_rates60_20190102_url = psp_isois_cda_base_url + '/2023/psp_isois-epihi_l2-het-rates60_20190102_v11.cdf'
        het_rates60_20190105_url = psp_isois_cda_base_url + '/2023/psp_isois-epihi_l2-het-rates60_20190105_v11.cdf'
        ephem_20181111_url = psp_isois_cda_base_url + '/2022/psp_isois_l2-ephem_20181111_v12.cdf'
        ephem_20181112_url = psp_isois_cda_base_url + '/2022/psp_isois_l2-ephem_20181112_v12.cdf'
        summary_20181114_url = psp_isois_cda_base_url + '/2023/psp_isois_l2-summary_20181114_v13.cdf'
        summary_20181115_url = psp_isois_cda_base_url + '/2023/psp_isois_l2-summary_20181115_v13.cdf'

        mock_downloader.get_url.side_effect = [
            het_rates3600_20190102_url,
            het_rates3600_20190103_url,
            het_rates60_20190102_url,
            het_rates60_20190105_url,
            ephem_20181111_url,
            ephem_20181112_url,
            summary_20181114_url,
            summary_20181115_url
        ]

        mock_get_with_retry.side_effect = [
            Mock(content=b'some data 1'),
            Mock(content=b'some data 2'),
            Mock(content=b'some data 3'),
            Mock(content=b'some data 4'),
        ]

        mock_cdf_parser.parse_cdf_bytes.side_effect = [
            CdfFileInfo(
                CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "PSP Description 10", "10", date(2022, 11, 14)),
                [CdfVariableInfo('a key into the CDF 1', 'a description v1', 'time_series', 'units')]),
            CdfFileInfo(
                CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "PSP Description 11", "11", date(2022, 11, 15)),
                [CdfVariableInfo('a key into the CDF 2', 'a description v2', 'time_series', 'units')]),
            CdfFileInfo(
                CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "PSP Description 12", "12", date(2022, 11, 16)),
                [CdfVariableInfo('a key into the CDF 3', 'a description v3', 'time_series', 'units')]),
            CdfFileInfo(
                CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "PSP Description 13", "13", date(2022, 11, 17)),
                [CdfVariableInfo('a key into the CDF 4', 'a description v4', 'time_series', 'units')]),

        ]

        actual_index = PspDataProcessor.get_metadata_index()

        mock_downloader.get_url.assert_has_calls([
            call(psp_isois_cda_base_url, 'psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf', 'epihi', 'het_rate1',
                 '2022'),
            call(psp_isois_cda_base_url, 'psp_isois-epihi_l2-het-rates3600_20190103_v10.cdf', 'epihi', 'het_rate1',
                 '2022'),
            call(psp_isois_cda_base_url, 'psp_isois-epihi_l2-het-rates60_20190102_v11.cdf', 'epihi', 'het_rate2',
                 '2023'),
            call(psp_isois_cda_base_url, 'psp_isois-epihi_l2-het-rates60_20190105_v11.cdf', 'epihi', 'het_rate2',
                 '2023'),
            call(psp_isois_cda_base_url, 'psp_isois_l2-ephem_20181111_v12.cdf', 'merged', 'ephem', '2022'),
            call(psp_isois_cda_base_url, 'psp_isois_l2-ephem_20181112_v12.cdf', 'merged', 'ephem', '2022'),
            call(psp_isois_cda_base_url, 'psp_isois_l2-summary_20181114_v13.cdf', 'merged', 'summary', '2023'),
            call(psp_isois_cda_base_url, 'psp_isois_l2-summary_20181115_v13.cdf', 'merged', 'summary', '2023')
        ])

        mock_get_with_retry.assert_has_calls([
            call(het_rates3600_20190103_url),
            call(het_rates60_20190105_url),
            call(ephem_20181112_url),
            call(summary_20181115_url),
        ])

        self.assertEqual([{"variables": [{'catalog_description': 'a description v1',
                                          'display_type': 'time_series',
                                          'variable_name': 'a key into the CDF 1', 'units': 'units'}],
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "logical_source_description": "PSP Description 10",
                           "generation_date": "2022-11-14",
                           "instrument": "ISOIS-EPIHi",
                           "mission": "PSP",
                           "file_cadence": "daily",
                           "file_timeranges": [
                               {
                                   "start_time": "2019-01-02T00:00:00+00:00",
                                   "end_time": "2019-01-03T00:00:00+00:00",
                                   "url": het_rates3600_20190102_url
                               },
                               {
                                   "start_time": "2019-01-03T00:00:00+00:00",
                                   "end_time": "2019-01-04T00:00:00+00:00",
                                   "url": het_rates3600_20190103_url
                               }
                           ]
                           },
                          {"variables": [{'catalog_description': 'a description v2',
                                          'display_type': 'time_series',
                                          'variable_name': 'a key into the CDF 2', 'units': 'units'}],
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "logical_source_description": "PSP Description 11",
                           "generation_date": "2022-11-15",
                           "instrument": "ISOIS-EPIHi",
                           "mission": "PSP",
                           "file_cadence": "daily",
                           "file_timeranges": [
                               {
                                   "start_time": "2019-01-02T00:00:00+00:00",
                                   "end_time": "2019-01-03T00:00:00+00:00",
                                   "url": het_rates60_20190102_url
                               },
                               {
                                   "start_time": "2019-01-05T00:00:00+00:00",
                                   "end_time": "2019-01-06T00:00:00+00:00",
                                   "url": het_rates60_20190105_url
                               }
                           ]
                           },
                          {"variables": [{'catalog_description': 'a description v3',
                                          'display_type': 'time_series',
                                          'variable_name': 'a key into the CDF 3', 'units': 'units'}],
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "logical_source_description": "PSP Description 12",
                           "generation_date": "2022-11-16",
                           "instrument": "ISOIS",
                           "mission": "Not PSP",
                           "file_cadence": "six_month",
                           "file_timeranges": [
                               {
                                   "start_time": "2018-07-01T00:00:00+00:00",
                                   "end_time": "2019-01-01T00:00:00+00:00",
                                   "url": ephem_20181111_url
                               },
                               {
                                   "start_time": "2018-07-01T00:00:00+00:00",
                                   "end_time": "2019-01-01T00:00:00+00:00",
                                   "url": ephem_20181112_url
                               }
                           ]
                           },
                          {"variables": [{'catalog_description': 'a description v4',
                                          'display_type': 'time_series',
                                          'variable_name': 'a key into the CDF 4', 'units': 'units'}],
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "logical_source_description": "PSP Description 13",
                           "generation_date": "2022-11-17",
                           "instrument": "ISOIS",
                           "mission": "Not PSP",
                           "file_cadence": "six_month",
                           "file_timeranges": [
                               {
                                   "start_time": "2018-07-01T00:00:00+00:00",
                                   "end_time": "2019-01-01T00:00:00+00:00",
                                   "url": summary_20181114_url
                               },
                               {
                                   "start_time": "2018-07-01T00:00:00+00:00",
                                   "end_time": "2019-01-01T00:00:00+00:00",
                                   "url": summary_20181115_url
                               }
                           ]
                           }],
                         actual_index)

        self.assertEqual(4, mock_cdf_parser.parse_cdf_bytes.call_count)
        self.assertEqual(b'some data 1', mock_cdf_parser.parse_cdf_bytes.call_args_list[0].args[0])
        self.assertEqual(b'some data 2', mock_cdf_parser.parse_cdf_bytes.call_args_list[1].args[0])
        self.assertEqual(b'some data 3', mock_cdf_parser.parse_cdf_bytes.call_args_list[2].args[0])
        self.assertEqual(b'some data 4', mock_cdf_parser.parse_cdf_bytes.call_args_list[3].args[0])

        self.assertEqual(sentinel.variable_selector_1, mock_cdf_parser.parse_cdf_bytes.call_args_list[0].args[1])
        self.assertEqual(sentinel.variable_selector_1, mock_cdf_parser.parse_cdf_bytes.call_args_list[1].args[1])

        self.assertEqual(sentinel.variable_selector_2, mock_cdf_parser.parse_cdf_bytes.call_args_list[2].args[1])
        self.assertEqual(sentinel.variable_selector_2, mock_cdf_parser.parse_cdf_bytes.call_args_list[3].args[1])
