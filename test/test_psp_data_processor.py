import unittest
from unittest.mock import patch, call

from src.cdf_downloader.psp_file_parser import PspFileInfo
from src.cdf_parser.cdf_global_parser import CdfGlobalInfo
from src.cdf_parser.cdf_parser import CdfFileInfo
from src.psp_data_processor import PspDataProcessor


class TestPspDataProcessor(unittest.TestCase):

    @patch('src.psp_data_processor.CdfParser')
    @patch('src.psp_data_processor.PspDownloader')
    def test_gets_filenames_and_downloads_the_first_file_in_list(self, mock_downloader, mock_cdf_parser):
        mock_downloader.get_all_filenames.return_value = {'epihi': {
            'het_rate1': [PspFileInfo('link1', 'psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf', '2022'),
                          PspFileInfo('link2', 'psp_isois-epihi_l2-het-rates3600_20190103_v10.cdf', '2022')],
            'het_rate2': [PspFileInfo('link3', 'psp_isois-epihi_l2-het-rates60_20190102_v11.cdf', '2023'),
                          PspFileInfo('link4', 'psp_isois-epihi_l2-het-rates60_20190105_v11.cdf', '2023')]},
            'merged': {
                'ephem': [PspFileInfo('linkfile_to_download5.cdf', 'psp_isois_l2-ephem_20181111_v12.cdf', '2022'),
                          PspFileInfo('linkfile_to_download6.cdf', 'psp_isois_l2-ephem_20181112_v12.cdf', '2022')],
                'summary': [PspFileInfo('linkfile_to_download7.cdf', 'psp_isois_l2-summary_20181114_v13.cdf', '2023'),
                            PspFileInfo('linkfile_to_download8.cdf', 'psp_isois_l2-summary_20181115_v13.cdf', '2023')]}
        }

        mock_downloader.get_cdf_file.side_effect = [
            {'link': '/2022/psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf', 'data': b'some data 1'},
            {'link': '/2023/psp_isois-epihi_l2-het-rates3600_20190102_v11.cdf', 'data': b'some data 2'},
            {'link': '/2022/psp_isois-epihi_l2-het-rates3600_20190102_v12.cdf', 'data': b'some data 3'},
            {'link': '/2023/psp_isois-epihi_l2-het-rates3600_20190102_v13.cdf', 'data': b'some data 4'},
        ]

        mock_cdf_parser.parse_cdf_bytes.side_effect = [
            CdfFileInfo(CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "10"),
                        {'a description v1': 'a key into the CDF 1'}),
            CdfFileInfo(CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "11"),
                        {'a description v2': 'a key into the CDF 2'}),
            CdfFileInfo(CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "12"),
                        {'a description v3': 'a key into the CDF 3'}),
            CdfFileInfo(CdfGlobalInfo("psp_isois-epihi_l2-het-rates3600", "13"),
                        {'a description v4': 'a key into the CDF 4'}),
        ]

        actual_index = PspDataProcessor.get_metadata_index()

        self.assertEqual([call('psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf', 'epihi', 'het_rate1', '2022'),
                          call('psp_isois-epihi_l2-het-rates60_20190102_v11.cdf', 'epihi', 'het_rate2', '2023'),
                          call('psp_isois_l2-ephem_20181111_v12.cdf', 'merged', 'ephem', '2022'),
                          call('psp_isois_l2-summary_20181114_v13.cdf', 'merged', 'summary', '2023')],
                         mock_downloader.get_cdf_file.call_args_list)

        self.assertEqual([{"descriptions": {'a description v1': 'a key into the CDF 1'},
                           "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v10.cdf',
                           "description_source_file": '/2022/psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf',
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "version": "10",
                           "dates_available": [["2019-01-02", "2019-01-03"]]},
                          {"descriptions": {'a description v2': 'a key into the CDF 2'},
                           "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v11.cdf',
                           "description_source_file": '/2023/psp_isois-epihi_l2-het-rates3600_20190102_v11.cdf',
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "version": "11",
                           "dates_available": [["2019-01-02", "2019-01-02"], ["2019-01-05", "2019-01-05"]]},
                          {"descriptions": {'a description v3': 'a key into the CDF 3'},
                           "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v12.cdf',
                           "description_source_file": '/2022/psp_isois-epihi_l2-het-rates3600_20190102_v12.cdf',
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "version": "12",
                           "dates_available": [["2018-11-11", "2018-11-12"]]},
                          {"descriptions": {'a description v4': 'a key into the CDF 4'},
                           "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v13.cdf',
                           "description_source_file": '/2023/psp_isois-epihi_l2-het-rates3600_20190102_v13.cdf',
                           "logical_source": "psp_isois-epihi_l2-het-rates3600",
                           "version": "13",
                           "dates_available": [["2018-11-14", "2018-11-15"]]}],
                         actual_index)

        self.assertEqual(4, mock_cdf_parser.parse_cdf_bytes.call_count)
        self.assertEqual(b'some data 1', mock_cdf_parser.parse_cdf_bytes.call_args_list[0].args[0])
        self.assertEqual(b'some data 2', mock_cdf_parser.parse_cdf_bytes.call_args_list[1].args[0])
        self.assertEqual(b'some data 3', mock_cdf_parser.parse_cdf_bytes.call_args_list[2].args[0])
        self.assertEqual(b'some data 4', mock_cdf_parser.parse_cdf_bytes.call_args_list[3].args[0])
