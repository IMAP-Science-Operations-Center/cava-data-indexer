from pathlib import Path
from unittest import TestCase
from unittest.mock import patch, call, MagicMock

from data_indexer.cdf_downloader.psp_file_parser import PspFileParser, PspFileInfo


class TestPspFileParser(TestCase):
    @patch('data_indexer.cdf_downloader.psp_file_parser.get_with_retry')
    def test_retrieves_list_of_files_for_types(self, mock_get_with_retry):
        mock_html_folder_path = Path(__file__).parent / 'mock_html/'
        file_path = mock_html_folder_path / 'l2.html'
        with open(file_path, 'r') as file:
            l2_html = file.read()
        file_path = mock_html_folder_path / 'het_rate_1.html'
        with open(file_path, 'r') as file:
            het_rate_1_html = file.read()

        file_path = mock_html_folder_path / 'het_rate_2.html'
        with open(file_path, 'r') as file:
            het_rate_2_html = file.read()

        file_path = mock_html_folder_path / '2022.html'
        with open(file_path, 'r') as file:
            twentytwo_html = file.read()

        file_path = mock_html_folder_path / '2022_rate2.html'
        with open(file_path, 'r') as file:
            twentytwo_r2_html = file.read()

        file_path = mock_html_folder_path / '2023.html'
        with open(file_path, 'r') as file:
            twentythree_html = file.read()

        mock_l2_response = MagicMock()
        mock_l2_response.__enter__.return_value = l2_html
        mock_l2_response.text = l2_html
        mock_het_rate_1_response = MagicMock()
        mock_het_rate_1_response.text = het_rate_1_html
        mock_het_rate_2_response = MagicMock()
        mock_het_rate_2_response.text = het_rate_2_html
        mock_2022_response = MagicMock()
        mock_2022_response.text = twentytwo_html
        mock_2022_r2_response = MagicMock()
        mock_2022_r2_response.text = twentytwo_r2_html
        mock_2023_response = MagicMock()
        mock_2023_response.text = twentythree_html

        mock_get_with_retry.side_effect = [mock_l2_response, mock_het_rate_1_response, mock_2022_response,
                                               mock_het_rate_2_response, mock_2022_r2_response, mock_2023_response]

        file_dictionary = PspFileParser.get_dictionary_of_files("https://url.site/l2/")

        mock_get_with_retry.assert_has_calls([
            call("https://url.site/l2/"),
            call("https://url.site/l2/het_rate_1/"),
            call("https://url.site/l2/het_rate_1/2022/"),
            call("https://url.site/l2/het_rate_2/"),
            call("https://url.site/l2/het_rate_2/2022_rate2/"),
            call("https://url.site/l2/het_rate_2/2023/"),
        ])

        expected_file_dictionary = {
            "het_rate_1/": [
                PspFileInfo("psp_isois-epihi_l2-het-rate1_20220928_v13.cdf",
                            "psp_isois-epihi_l2-het-rate1_20220928_v13.cdf", "2022"),
                PspFileInfo("psp_isois-epihi_l2-het-rate1_20220929_v13.cdf",
                            "psp_isois-epihi_l2-het-rate1_20220929_v13.cdf", "2022")],
            "het_rate_2/": [
                PspFileInfo("psp_isois-epihi_l2-het-rate2_20220928_v13.cdf",
                            "psp_isois-epihi_l2-het-rate2_20220928_v13.cdf", "2022_rate2"),
                PspFileInfo("psp_isois-epihi_l2-het-rate2_20220929_v13.cdf",
                            "psp_isois-epihi_l2-het-rate2_20220929_v13.cdf", "2022_rate2"),
                PspFileInfo("psp_isois-epihi_l2-het-rate2_20230928_v13.cdf",
                            "psp_isois-epihi_l2-het-rate2_20230928_v13.cdf", "2023"),
                PspFileInfo("psp_isois-epihi_l2-het-rate2_20230929_v13.cdf",
                            "psp_isois-epihi_l2-het-rate2_20230929_v13.cdf", "2023")
            ]
        }

        self.assertEqual(expected_file_dictionary, file_dictionary)
