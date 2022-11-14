from unittest import TestCase
from unittest.mock import patch, Mock, call

from src.cdf_downloader.psp_downloader import PspDownloader
from src.cdf_downloader.psp_file_parser import PspFileParser


class TestPspDownloader(TestCase):
    @patch('src.cdf_downloader.psp_downloader.PspFileParser')
    def test_download_psp_data_from_cda_web(self, mock_psp_file_parser: PspFileParser):
        epihi_filenames = {"het_rate_1/": ["psp_isois-epihi_l2-het-rate1_20220928_v13.cdf",
                                           "psp_isois-epihi_l2-het-rate1_20220929_v13.cdf"],
                           "het_rate_2/": ["psp_isois-epihi_l2-het-rate2_20220928_v13.cdf",
                                           "psp_isois-epihi_l2-het-rate2_20220929_v13.cdf"]}

        epilo_filenames = {"ic": ["epilo_1.cdf",
                                  "epilo_2.cdf"], }

        merged_filenames = {"summary": ["summary_1.cdf"]}

        mock_psp_file_parser.get_dictionary_of_files.side_effect = [epihi_filenames, epilo_filenames, merged_filenames]

        metadata = PspDownloader.get_all_filenames()

        expected_metadata = {"epihi": epihi_filenames, "epilo": epilo_filenames, "merged": merged_filenames}

        self.assertEqual(expected_metadata, metadata)
        mock_psp_file_parser.get_dictionary_of_files.assert_any_call('https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois'
                                                                     '/epihi/l2/')
        mock_psp_file_parser.get_dictionary_of_files.assert_any_call(
            'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/epilo/l2/')
        mock_psp_file_parser.get_dictionary_of_files.assert_any_call(
            'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/merged/l2/')

    @patch('src.cdf_downloader.psp_downloader.urllib')
    def test_download_individual_file(self, mock_urllib):
        mock_file_download_response = Mock()
        mock_file_download_response.read.return_value = b'This is a PSP file'
        mock_urllib.request.urlopen.return_value = mock_file_download_response

        response = PspDownloader.get_cdf_file("psp_isois-epihi_l2-het-rate1_20220928_v13.cdf", "epihi", "het-rate1/",
                                              "2022")

        self.assertEqual([call(
            "https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/epihi/l2/het-rate1/2022/psp_isois-epihi_l2-het-rate1_20220928_v13.cdf")],
                         mock_urllib.request.urlopen.call_args_list)
        self.assertEqual({
                             "link": "https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/epihi/l2/het-rate1/2022/psp_isois-epihi_l2-het-rate1_20220928_v13.cdf",
                             "data": b'This is a PSP file'}, response)
