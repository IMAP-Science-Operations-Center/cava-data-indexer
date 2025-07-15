import uuid
from datetime import date
from unittest import TestCase
from unittest.mock import patch, call, Mock

import spacepy.pycdf.const
from spacepy.pycdf import CDFError

from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalInfo
from data_indexer.cdf_parser.cdf_parser import CdfFileInfo
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.imap_data_processor import get_metadata_index, imap_dev_server


class TestImapDataProcessor(TestCase):
    @patch('data_indexer.imap_data_processor.CdfParser')
    @patch('data_indexer.imap_data_processor.imap_data_access.query')
    @patch('data_indexer.imap_data_processor.get_with_retry')
    def test_get_metadata_index(self, mock_get_with_retry, mock_data_access_query, mock_cdf_parser):

        l3a_protons_data_product_v3 = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf'
        l3a_protons_data_product_v2_outdated = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v002.cdf'

        l3a_pui_data_product_20250606_v3 = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250606_v003.cdf'
        l3a_pui_data_product_20250607_v2 = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250607_v002.cdf'

        l3a_pui_diff_instrument_product = 'fake-mission/fake-instrument2/l3a/2025/06/fake-mission_fake-instrument2_l3a_pui-he_20250607_v003.cdf'
        l3b_pui_data_product = 'fake-mission/fake-instrument/l3b/2025/06/fake-mission_fake-instrument_l3b_pui-he_20250607_v003.cdf'

        mock_data_access_query.return_value = [
            {
                'file_path': l3a_protons_data_product_v2_outdated,
                'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': 'protons', 'start_date': '20250606', 'repointing': None,
                'version': 'v002', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:59'},

            {
                'file_path': l3a_protons_data_product_v3, 'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': 'protons', 'start_date': '20250606', 'repointing': None,
                'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:58'
            },
            {
                'file_path': l3a_pui_data_product_20250607_v2, 'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250607', 'repointing': None,
                'version': 'v002', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:12:40'
            },
            {
                'file_path': l3a_pui_data_product_20250606_v3, 'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250606', 'repointing': None,
                'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:59'
            },

            {
                'file_path': l3a_pui_diff_instrument_product, 'instrument': 'fake-instrument2',
                'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250607', 'repointing': None,
                'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:12:40'
             },
            {
                'file_path': l3b_pui_data_product, 'instrument': 'fake-instrument',
                'data_level': 'l3b', 'descriptor': 'pui-he', 'start_date': '20250607', 'repointing': None,
                'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:12:40'
            }
        ]

        first_cdf_response = Mock()
        second_cdf_response = Mock()
        third_cdf_response = Mock()
        fourth_cdf_response = Mock()

        mock_get_with_retry.side_effect = [first_cdf_response, second_cdf_response, third_cdf_response, fourth_cdf_response]

        mock_cdf_parser.parse_cdf_bytes.side_effect = [
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument_l3a_protons", "Parker Solar Probe Level 2 Summary",
                                      "1.27.0",
                                      date(2022, 11, 12)),
                        [CdfVariableInfo("VAR1", "variable 1", "time-series", None),
                         CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units")]),
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument_l3a_pui-he", "Parker Solar Probe Level 2 Ephemeris",
                                      "1.27.0",
                                      date(2022, 11, 13)),
                        [CdfVariableInfo("VAR3", "variable 3", "time-series", None),
                         CdfVariableInfo("VAR4", "variable 4", "spectrogram", None)]),
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument2_l3a_pui-he", "Parker Solar Probe Level 2 Summary",
                                      "1.27.0",
                                      date(2022, 11, 12)),
                        [CdfVariableInfo("VAR1", "variable 1", "time-series", None),
                         CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units")]),
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument_l3b_pui-he", "Parker Solar Probe Level 2 Ephemeris",
                                      "1.27.0",
                                      date(2022, 11, 13)),
                        [CdfVariableInfo("VAR3", "variable 3", "time-series", None),
                         CdfVariableInfo("VAR4", "variable 4", "spectrogram", None)])
        ]

        actual_index = get_metadata_index()

        l3a_protons_data_product_v3_url = imap_dev_server + f"download/{l3a_protons_data_product_v3}"
        l3a_pui_data_product_20250606_v3_url = imap_dev_server + f"download/{l3a_pui_data_product_20250606_v3}"
        l3a_pui_data_product_20250607_v2_url = imap_dev_server + f"download/{l3a_pui_data_product_20250607_v2}"
        l3a_pui_diff_instrument_product_url = imap_dev_server + f"download/{l3a_pui_diff_instrument_product}"
        l3b_pui_data_product_url = imap_dev_server + f"download/{l3b_pui_data_product}"

        mock_get_with_retry.assert_has_calls([
            call(l3a_protons_data_product_v3_url),
            call(l3a_pui_data_product_20250607_v2_url),
            call(l3a_pui_diff_instrument_product_url),
            call(l3b_pui_data_product_url)
        ])

        expected_index = [
            {
                "file_timeranges": [{
                    "start_time": "2025-06-06T00:00:00+00:00",
                    "end_time": "2025-06-07T00:00:00+00:00",
                    "url": l3a_protons_data_product_v3_url
                }],
                "logical_source": "fake-mission_fake-instrument_l3a_protons",
                "logical_source_description": "Parker Solar Probe Level 2 Summary",
                "variables": [{'catalog_description': 'variable 1',
                               'display_type': 'time-series',
                               'variable_name': 'VAR1', 'units': None},
                              {'catalog_description': 'variable 2',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR2', 'units': "Units"}],
                "generation_date": "2022-11-12",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
            },
            {
                "file_timeranges": [
                    {
                        "start_time": "2025-06-06T00:00:00+00:00",
                        "end_time": "2025-06-07T00:00:00+00:00",
                        "url": l3a_pui_data_product_20250606_v3_url
                    },
                    {
                        "start_time": "2025-06-07T00:00:00+00:00",
                        "end_time": "2025-06-08T00:00:00+00:00",
                        "url": l3a_pui_data_product_20250607_v2_url
                    }
                ],
                "logical_source": "fake-mission_fake-instrument_l3a_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Ephemeris",
                "variables": [{'catalog_description': 'variable 3',
                               'display_type': 'time-series',
                               'variable_name': 'VAR3', 'units': None},
                              {'catalog_description': 'variable 4',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR4', 'units': None}],
                "generation_date": "2022-11-13",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
            },
            {
                "file_timeranges": [
                    {
                        "start_time": "2025-06-07T00:00:00+00:00",
                        "end_time": "2025-06-08T00:00:00+00:00",
                        "url": l3a_pui_diff_instrument_product_url
                    },
                ],
                "logical_source": "fake-mission_fake-instrument2_l3a_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Summary",
                "variables": [{'catalog_description': 'variable 1',
                               'display_type': 'time-series',
                               'variable_name': 'VAR1', 'units': None},
                              {'catalog_description': 'variable 2',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR2', 'units': "Units"}],
                "generation_date": "2022-11-12",
                "instrument": "fake-instrument2",
                "mission": "IMAP",
                "file_cadence": "daily",
            },
            {
                "file_timeranges": [
                    {
                        "start_time": "2025-06-07T00:00:00+00:00",
                        "end_time": "2025-06-08T00:00:00+00:00",
                        "url": l3b_pui_data_product_url
                    },
                ],
                "logical_source": "fake-mission_fake-instrument_l3b_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Ephemeris",
                "variables": [{'catalog_description': 'variable 3',
                               'display_type': 'time-series',
                               'variable_name': 'VAR3', 'units': None},
                              {'catalog_description': 'variable 4',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR4', 'units': None}],
                "generation_date": "2022-11-13",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
            }
        ]

        mock_data_access_query.assert_has_calls([
            call(data_level="l3"),
            call(data_level="l3a"),
            call(data_level="l3b"),
            call(data_level="l3c"),
            call(data_level="l3d"),
            call(data_level="l3e"),
        ])
        self.assertEqual(expected_index, actual_index)

        self.assertEqual([
            call(first_cdf_response.content, DefaultVariableSelector),
            call(second_cdf_response.content, DefaultVariableSelector),
            call(third_cdf_response.content, DefaultVariableSelector),
            call(fourth_cdf_response.content, DefaultVariableSelector)],
            mock_cdf_parser.parse_cdf_bytes.call_args_list)

    @patch('data_indexer.imap_data_processor.CdfParser')
    @patch('data_indexer.imap_data_processor.imap_data_access.query')
    @patch('data_indexer.imap_data_processor.get_with_retry')
    def test_get_metadata_index_returns_instrument_names_correctly(self, _mock_get_with_retry, mock_data_access_query, _):
        lowercase_instruments = ["codice", "glows", "hi", "hit", "idex", "lo", "mag", "swapi", "swe", "ultra", ]
        uppercase_instruments = [
            "CoDICE",
            "GLOWS",
            "IMAP-Hi",
            "HIT",
            "IDEX",
            "IMAP-Lo",
            "MAG",
            "SWAPI",
            "SWE",
            "IMAP-Ultra",
        ]
        mock_data_access_query.return_value = [
            {'file_path': '', 'instrument': instrument,
             'data_level': 'l3a', 'descriptor': 'protons', 'start_date': '20250606', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:58'}
            for instrument in lowercase_instruments
        ]

        actual_index = get_metadata_index()

        self.assertEqual(10, len(actual_index))

        for expected, actual in zip(uppercase_instruments, actual_index):
            self.assertEqual(expected, actual["instrument"])

    @patch('data_indexer.imap_data_processor.imap_data_access.query')
    def test_get_metadata_index_excludes_log_files_and_files_with_uuids(self, mock_data_access_query):
        mock_data_access_query.return_value = [
            {
                'file_path': 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v002.cdf',
                'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': 'this is a log file', 'start_date': '20250606', 'repointing': None,
                'version': 'v002', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:59'},
            {
                'file_path': 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf',
                'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': f'the id is {uuid.uuid4()}', 'start_date': '20250606',
                'repointing': None,
                'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:58'},
        ]

        actual_index = get_metadata_index()

        mock_data_access_query.assert_called()
        self.assertEqual([], actual_index)

    @patch('data_indexer.imap_data_processor.CdfParser')
    @patch('data_indexer.imap_data_processor.imap_data_access.query')
    @patch('data_indexer.imap_data_processor.get_with_retry')
    def test_ignores_poorly_formatted_cdfs(self, _, mock_imap_query,
                                           mock_cdf_parser):
        expected_file_path_1 = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf'
        expected_file_path_2 = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250606_v003.cdf'

        mock_imap_query.return_value = [{'file_path': expected_file_path_1, 'instrument': 'fake-instrument',
                                         'data_level': 'l3a', 'descriptor': 'protons', 'start_date': '20250606',
                                         'repointing': None,
                                         'version': 'v003', 'extension': 'cdf',
                                         'ingestion_date': '2024-11-21 21:09:58'},
                                        {'file_path': expected_file_path_2, 'instrument': 'fake-instrument',
                                         'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250606',
                                         'repointing': None,
                                         'version': 'v003', 'extension': 'cdf',
                                         'ingestion_date': '2024-11-21 21:09:59'}, ]

        mock_cdf_parser.parse_cdf_bytes.side_effect = [CDFError(spacepy.pycdf.const.NOT_A_CDF_OR_NOT_SUPPORTED),
                                                       CdfFileInfo(
                                                           CdfGlobalInfo("fake-mission_fake-instrument_l3a_protons",
                                                                         "Parker Solar Probe Level 2 Summary",
                                                                         "1.27.0",
                                                                         date(2022, 11, 12)),
                                                           [CdfVariableInfo("VAR1", "variable 1", "time-series", None),
                                                            CdfVariableInfo("VAR2", "variable 2", "spectrogram",
                                                                            "Units")]), ]
        actual_index = get_metadata_index()

        self.assertEqual(1, len(actual_index))
