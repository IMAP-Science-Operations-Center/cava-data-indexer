from unittest import TestCase
from unittest.mock import patch, Mock, call

import certifi

from data_indexer.cdf_downloader.psp_downloader import PspDownloader, PspDirectoryInfo, psp_isois_cda_base_url, \
    psp_fields_cda_base_url
from data_indexer.cdf_downloader.psp_file_parser import PspFileParser, PspFileInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.cdf_parser.variable_selector.multi_dimension_variable_selector import MultiDimensionVariableSelector


class TestPspDownloader(TestCase):
    @patch('data_indexer.cdf_downloader.psp_downloader.PspFileParser')
    def test_download_psp_data_from_cda_web(self, mock_psp_file_parser: PspFileParser):
        epihi_filenames = {"het_rate_1/": [PspFileInfo("psp_isois-epihi_l2-het-rate1_20220928_v13.cdf", "name", "1998"),
                                           PspFileInfo("psp_isois-epihi_l2-het-rate1_20220929_v13.cdf", "name", "1999")],
                           "het_rate_2/": [PspFileInfo("psp_isois-epihi_l2-het-rate2_20220928_v13.cdf", "name", "2000"),
                                           PspFileInfo("psp_isois-epihi_l2-het-rate2_20220929_v13.cdf", "name", "2001")]}

        epilo_filenames = {"ic": [PspFileInfo("epilo_1.cdf", "name5", "2002"),
                                  PspFileInfo("epilo_2.cdf", "name6", "2003")], }

        merged_filenames = {"summary": [PspFileInfo("summary_1.cdf", "name7", "2004")]}

        fields_filenames = {"": [
            PspFileInfo("psp_fld_l2_mag_rtn_4_sa_per_cyc_20230101_v02.cdf", "psp_fld_l2_mag_rtn_4_sa_per_cyc_20230101_v02", "2023"),
            PspFileInfo("psp_fld_l2_mag_rtn_4_sa_per_cyc_20230102_v02.cdf", "psp_fld_l2_mag_rtn_4_sa_per_cyc_20230102_v02", "2023"),
            PspFileInfo("psp_fld_l2_mag_rtn_4_sa_per_cyc_20220101_v02.cdf", "psp_fld_l2_mag_rtn_4_sa_per_cyc_20220101_v02", "2022"),
            PspFileInfo("psp_fld_l2_mag_rtn_4_sa_per_cyc_20220102_v02.cdf", "psp_fld_l2_mag_rtn_4_sa_per_cyc_20220102_v02", "2022"),
            PspFileInfo("psp_fld_l2_mag_rtn_4_sa_per_cyc_20210101_v02.cdf", "psp_fld_l2_mag_rtn_4_sa_per_cyc_20210101_v02", "2021")
        ]}

        mock_psp_file_parser.get_dictionary_of_files.side_effect = [epihi_filenames, epilo_filenames, merged_filenames, fields_filenames]

        metadata = PspDownloader.get_all_metadata()

        expected_metadata = [PspDirectoryInfo(psp_isois_cda_base_url, "ISOIS-EPIHi", "epihi", epihi_filenames, DefaultVariableSelector),
                             PspDirectoryInfo(psp_isois_cda_base_url, "ISOIS-EPILo", "epilo", epilo_filenames, MultiDimensionVariableSelector),
                             PspDirectoryInfo(psp_isois_cda_base_url, "ISOIS", "merged", merged_filenames, DefaultVariableSelector),
                             PspDirectoryInfo(psp_fields_cda_base_url, 'FIELDS', 'mag_rtn_4_per_cycle', fields_filenames, MultiDimensionVariableSelector)
                             ]

        self.assertEqual(expected_metadata, metadata)
        mock_psp_file_parser.get_dictionary_of_files.assert_any_call('https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois'
                                                                     '/epihi/l2/')
        mock_psp_file_parser.get_dictionary_of_files.assert_any_call(
            'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/epilo/l2/')
        mock_psp_file_parser.get_dictionary_of_files.assert_any_call(
            'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/merged/l2/')
        mock_psp_file_parser.get_dictionary_of_files.assert_any_call(
            'https://cdaweb.gsfc.nasa.gov/pub/data/psp/fields/l2/mag_rtn_4_per_cycle/', top_level_link="")

    @patch('data_indexer.cdf_downloader.psp_downloader.ssl.create_default_context')
    @patch('data_indexer.cdf_downloader.psp_downloader.urllib')
    def test_download_individual_file(self, mock_urllib, mock_create_default_context):
        mock_file_download_response = Mock()
        mock_file_download_response.read.return_value = b'This is a PSP file'
        mock_urllib.request.urlopen.return_value = mock_file_download_response

        response = PspDownloader.get_cdf_file(psp_isois_cda_base_url, "psp_isois-epihi_l2-het-rate1_20220928_v13.cdf", "epihi", "het-rate1/",
                                              "2022")

        mock_create_default_context.assert_called_with(cafile=certifi.where())

        self.assertEqual([call(
            "https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/epihi/l2/het-rate1/2022/psp_isois-epihi_l2-het-rate1_20220928_v13.cdf",
            context=mock_create_default_context.return_value)],
            mock_urllib.request.urlopen.call_args_list)
        self.assertEqual({
            "link": "https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/epihi/l2/het-rate1/2022/psp_isois-epihi_l2-het-rate1_20220928_v13.cdf",
            "data": b'This is a PSP file'}, response)
