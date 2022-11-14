import unittest
from unittest.mock import patch, call

from src.cdf_downloader.psp_file_parser import PspFileInfo
from src.psp_data_processor import PspDataProcessor


class TestPspDataProcessor(unittest.TestCase):

    @patch('src.psp_data_processor.PspDownloader')
    @patch('src.psp_data_processor.CdfVariableParser')
    def test_gets_filenames_and_downloads_the_first_file_in_list(self, mock_cdf_variable_parser, mock_downloader):
        mock_downloader.get_all_filenames.return_value = {'epihi': {
            'het_rate1': [PspFileInfo('linkfile_to_download1.cdf', 'file_to_download1.cdf', '2022'),
                          PspFileInfo('linkfile_to_download2.cdf', 'file_to_download2.cdf', '2022')],
            'het_rate2': [PspFileInfo('linkfile_to_download3.cdf', 'file_to_download3.cdf', '2023'),
                          PspFileInfo('linkfile_to_download4.cdf', 'file_to_download4.cdf', '2023')]},
            'merged': {
                'ephem': [PspFileInfo('linkfile_to_download5.cdf', 'file_to_download5.cdf', '2022'),
                          PspFileInfo('linkfile_to_download6.cdf', 'file_to_download6.cdf', '2022')],
                'summary': [PspFileInfo('linkfile_to_download7.cdf', 'file_to_download7.cdf', '2023'),
                            PspFileInfo('linkfile_to_download8.cdf', 'file_to_download8.cdf', '2023')]}
        }

        mock_downloader.get_cdf_file.side_effect = [
            {'link': '/2022/psp_isois-epihi_l2-het-rates3600_20190102_v10.cdf', 'data': b'some data 1'},
            {'link': '/2023/psp_isois-epihi_l2-het-rates3600_20190102_v11.cdf', 'data': b'some data 2'},
            {'link': '/2022/psp_isois-epihi_l2-het-rates3600_20190102_v12.cdf', 'data': b'some data 3'},
            {'link': '/2023/psp_isois-epihi_l2-het-rates3600_20190102_v13.cdf', 'data': b'some data 4'},
        ]

        mock_cdf_variable_parser.parse_variables_from_cdf_bytes.side_effect = [
            {'a description v1': 'a key into the CDF 1'},
            {'a description v2': 'a key into the CDF 2'},
            {'a description v3': 'a key into the CDF 3'},
            {'a description v4': 'a key into the CDF 4'}]

        actual_index = PspDataProcessor.get_metadata_index()

        self.assertEqual([call('file_to_download1.cdf', 'epihi', 'het_rate1', '2022'),
                          call('file_to_download3.cdf', 'epihi', 'het_rate2', '2023'),
                          call('file_to_download5.cdf', 'merged', 'ephem', '2022'),
                          call('file_to_download7.cdf', 'merged', 'summary', '2023')],
                         mock_downloader.get_cdf_file.call_args_list)

        self.assertEqual([
            call(b'some data 1'),
            call(b'some data 2'),
            call(b'some data 3'),
            call(b'some data 4'),
        ],
            mock_cdf_variable_parser.parse_variables_from_cdf_bytes.call_args_list)

        self.assertEqual([{"descriptions": {'a description v1': 'a key into the CDF 1'}, "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v10.cdf'},
                          {"descriptions": {'a description v2': 'a key into the CDF 2'}, "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v11.cdf'},
                          {"descriptions": {'a description v3': 'a key into the CDF 3'}, "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v12.cdf'},
                          {"descriptions": {'a description v4': 'a key into the CDF 4'}, "source_file_format": '/%yyyy%/psp_isois-epihi_l2-het-rates3600_%yyyymmdd%_v13.cdf'}],
                         actual_index)
