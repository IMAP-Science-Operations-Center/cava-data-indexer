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
    @patch('data_indexer.imap_data_processor.urllib.request.urlopen')
    def test_get_metadata_index(self, mock_url_open, mock_data_access_query, mock_cdf_parser):
        expected_file_path_1 = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf'
        expected_file_path_2 = 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250606_v003.cdf'
        expected_file_path_3 = 'fake-mission/fake-instrument2/l3a/2025/06/fake-mission_fake-instrument2_l3a_pui-he_20250607_v003.cdf'
        expected_file_path_4 = 'fake-mission/fake-instrument/l3b/2025/06/fake-mission_fake-instrument_l3b_pui-he_20250607_v003.cdf'
        mock_data_access_query.return_value = [
            {
                'file_path': 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v002.cdf',
                'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': 'protons', 'start_date': '20250606', 'repointing': None,
                'version': 'v002', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:59'},
            {'file_path': expected_file_path_1, 'instrument': 'fake-instrument',
             'data_level': 'l3a', 'descriptor': 'protons', 'start_date': '20250606', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:58'},
            {'file_path': expected_file_path_2, 'instrument': 'fake-instrument',
             'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250606', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:59'},
            {
                'file_path': 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250607_v003.cdf',
                'instrument': 'fake-instrument',
                'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250607', 'repointing': None,
                'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:12:40'},
            {'file_path': expected_file_path_3,
             'instrument': 'fake-instrument2',
             'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250607', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:12:40'},
            {'file_path': expected_file_path_4,
             'instrument': 'fake-instrument',
             'data_level': 'l3b', 'descriptor': 'pui-he', 'start_date': '20250607', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:12:40'}
        ]

        first_cdf_response = Mock()
        second_cdf_response = Mock()
        third_cdf_response = Mock()
        fourth_cdf_response = Mock()

        mock_url_open.side_effect = [first_cdf_response, second_cdf_response, third_cdf_response, fourth_cdf_response]

        mock_cdf_parser.parse_cdf_bytes.side_effect = [
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument_l3a_protons", "Parker Solar Probe Level 2 Summary",
                                      "1.27.0",
                                      date(2022, 11, 12)),
                        [CdfVariableInfo("VAR1", "variable 1", "time-series", None,"axis_label_1"),
                         CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units","axis_label_2")]),
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument_l3a_pui-he", "Parker Solar Probe Level 2 Ephemeris",
                                      "1.27.0",
                                      date(2022, 11, 13)),
                        [CdfVariableInfo("VAR3", "variable 3", "time-series", None,"axis_label_3"),
                         CdfVariableInfo("VAR4", "variable 4", "spectrogram", None,"axis_label_4")]),
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument2_l3a_pui-he", "Parker Solar Probe Level 2 Summary",
                                      "1.27.0",
                                      date(2022, 11, 12)),
                        [CdfVariableInfo("VAR1", "variable 1", "time-series", None,"axis_label_1"),
                         CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units","axis_label_2")]),
            CdfFileInfo(CdfGlobalInfo("fake-mission_fake-instrument_l3b_pui-he", "Parker Solar Probe Level 2 Ephemeris",
                                      "1.27.0",
                                      date(2022, 11, 13)),
                        [CdfVariableInfo("VAR3", "variable 3", "time-series", None,"axis_label_3"),
                         CdfVariableInfo("VAR4", "variable 4", "spectrogram", None,"axis_label_4")])
        ]

        actual_index = get_metadata_index()

        expected_description_source_file_url_1 = imap_dev_server + f"download/{expected_file_path_1}"
        expected_description_source_file_url_2 = imap_dev_server + f"download/{expected_file_path_2}"
        expected_description_source_file_url_3 = imap_dev_server + f"download/{expected_file_path_3}"
        expected_description_source_file_url_4 = imap_dev_server + f"download/{expected_file_path_4}"

        mock_url_open.assert_has_calls([call(expected_description_source_file_url_1),
                                        call(expected_description_source_file_url_2),
                                        call(expected_description_source_file_url_3),
                                        call(expected_description_source_file_url_4)])

        expected_index = [
            {
                "logical_source": "fake-mission_fake-instrument_l3a_protons",
                "logical_source_description": "Parker Solar Probe Level 2 Summary",
                "version": "1.27.0",
                "dates_available": [],
                "variables": [{'catalog_description': 'variable 1',
                               'display_type': 'time-series',
                               'variable_name': 'VAR1', 'units': None, "axis_label":"axis_label_1"},
                              {'catalog_description': 'variable 2',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR2', 'units': "Units","axis_label":"axis_label_2"}],
                "source_file_format": "https://api.dev.imap-mission.com/download/fake-mission/fake-instrument/l3a/%yyyy%/%mm%/fake-mission_fake-instrument_l3a_protons_%yyyymmdd%_v003.cdf",
                "description_source_file": expected_description_source_file_url_1,
                "generation_date": "2022-11-12",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
            },
            {
                "logical_source": "fake-mission_fake-instrument_l3a_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Ephemeris",
                "version": "1.27.0",
                "dates_available": [],
                "variables": [{'catalog_description': 'variable 3',
                               'display_type': 'time-series',
                               'variable_name': 'VAR3', 'units': None,"axis_label":"axis_label_3"},
                              {'catalog_description': 'variable 4',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR4', 'units': None,"axis_label":"axis_label_4"}],
                "source_file_format": "https://api.dev.imap-mission.com/download/fake-mission/fake-instrument/l3a/%yyyy%/%mm%/fake-mission_fake-instrument_l3a_pui-he_%yyyymmdd%_v003.cdf",
                "description_source_file": expected_description_source_file_url_2,
                "generation_date": "2022-11-13",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
            },
            {
                "logical_source": "fake-mission_fake-instrument2_l3a_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Summary",
                "version": "1.27.0",
                "dates_available": [],
                "variables": [{'catalog_description': 'variable 1',
                               'display_type': 'time-series',
                               'variable_name': 'VAR1', 'units': None,"axis_label":"axis_label_1"},
                              {'catalog_description': 'variable 2',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR2', 'units': "Units","axis_label":"axis_label_2"}],
                "source_file_format": "https://api.dev.imap-mission.com/download/fake-mission/fake-instrument2/l3a/%yyyy%/%mm%/fake-mission_fake-instrument2_l3a_pui-he_%yyyymmdd%_v003.cdf",
                "description_source_file": expected_description_source_file_url_3,
                "generation_date": "2022-11-12",
                "instrument": "fake-instrument2",
                "mission": "IMAP",
                "file_cadence": "daily",
            },
            {
                "logical_source": "fake-mission_fake-instrument_l3b_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Ephemeris",
                "version": "1.27.0",
                "dates_available": [],
                "variables": [{'catalog_description': 'variable 3',
                               'display_type': 'time-series',
                               'variable_name': 'VAR3', 'units': None,"axis_label":"axis_label_3"},
                              {'catalog_description': 'variable 4',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR4', 'units': None,"axis_label":"axis_label_4"}],
                "source_file_format": "https://api.dev.imap-mission.com/download/fake-mission/fake-instrument/l3b/%yyyy%/%mm%/fake-mission_fake-instrument_l3b_pui-he_%yyyymmdd%_v003.cdf",
                "description_source_file": expected_description_source_file_url_4,
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
            call(first_cdf_response.read.return_value, DefaultVariableSelector),
            call(second_cdf_response.read.return_value, DefaultVariableSelector),
            call(third_cdf_response.read.return_value, DefaultVariableSelector),
            call(fourth_cdf_response.read.return_value, DefaultVariableSelector)],
            mock_cdf_parser.parse_cdf_bytes.call_args_list)

    @patch('data_indexer.imap_data_processor.CdfParser')
    @patch('data_indexer.imap_data_processor.imap_data_access.query')
    @patch('data_indexer.imap_data_processor.urllib.request.urlopen')
    def test_get_metadata_index_returns_instrument_names_correctly(self, mock_url_open, mock_data_access_query, _):
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

        mock_url_open.return_value = Mock()

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
    @patch('data_indexer.imap_data_processor.urllib.request.urlopen')
    def test_ignores_poorly_formatted_cdfs(self, mock_url_open, mock_imap_query,
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
                                                           [CdfVariableInfo("VAR1", "variable 1", "time-series", None,"axis"),
                                                            CdfVariableInfo("VAR2", "variable 2", "spectrogram",
                                                                            "Units","Axis")]), ]
        actual_index = get_metadata_index()

        self.assertEqual(1, len(actual_index))
