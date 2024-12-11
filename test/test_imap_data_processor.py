from datetime import date
from unittest import TestCase
from unittest.mock import patch, call

from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalInfo
from data_indexer.cdf_parser.cdf_parser import CdfFileInfo
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.imap_data_processor import group_metadata_by_file_names, get_metadata_index


class TestImapDataProcessor(TestCase):
    def test_group_metadata_by_file_names(self):
        metadata_summary_1102 = {"file_name": "psp_isois_l2-summary_20181102_v1.27.0.cdf",
                                 "timetag": "2018-11-02 00:00:00+00:00"}
        metadata_summary_1103 = {"file_name": "psp_isois_l2-summary_20181103_v1.27.0.cdf",
                                 "timetag": "2018-11-03 00:00:00+00:00"}
        metadata_ephem_1102 = {"file_name": "psp_isois_l2-ephem_20181102_v1.27.0.cdf",
                               "timetag": "2018-11-02 00:00:00+00:00"}
        metadata_imap = {"file_name": "imap_super100000000_ql_20210613T035100_0000_xos1_vid_r_v01-01.cdf",
                         "timetag": "2021-06-13 00:00:00+00:00"}
        metadata = [
            metadata_summary_1102,
            metadata_summary_1103,
            metadata_ephem_1102,
            metadata_imap
        ]

        expected_output = {
            "psp_isois_l2-summary_%yyyymmdd%_v1.27.0.cdf": [metadata_summary_1102, metadata_summary_1103],
            "psp_isois_l2-ephem_%yyyymmdd%_v1.27.0.cdf": [metadata_ephem_1102],
            "imap_super100000000_ql_%yyyymmdd%T035100_0000_xos1_vid_r_v01-01.cdf": [metadata_imap]
        }

        actual_output = group_metadata_by_file_names(metadata)
        self.assertEqual(expected_output, actual_output)

    @patch('data_indexer.imap_data_processor.CdfParser')
    @patch('data_indexer.imap_data_processor.get_all_metadata')
    @patch('data_indexer.imap_data_processor.get_cdf_file_from_sdc')
    def test_get_metadata_index(self, mock_get_cdf_file, mock_get_all_metadata, mock_cdf_parser):
        mock_get_all_metadata.return_value = [
            {'file_path': 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_sw-particles_20250606_v003.cdf', 'instrument': 'fake-instrument',
             'data_level': 'l3a', 'descriptor': 'sw-particles', 'start_date': '20250606', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:59'},
            {'file_path': 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf', 'instrument': 'fake-instrument',
             'data_level': 'l3a', 'descriptor': 'protons', 'start_date': '20250606', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:58'},
            {'file_path': 'fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250606_v003.cdf', 'instrument': 'fake-instrument',
             'data_level': 'l3a', 'descriptor': 'pui-he', 'start_date': '20250606', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:09:59'},
            {'file_path': 'fake-mission/fake-instrument/l3b/2025/06/fake-mission_fake-instrument_l3b_combined_20250606_v003.cdf', 'instrument': 'fake-instrument',
             'data_level': 'l3b', 'descriptor': 'combined', 'start_date': '20250606', 'repointing': None,
             'version': 'v003', 'extension': 'cdf', 'ingestion_date': '2024-11-21 21:12:40'}
        ]

        first_cdf_file_data = b'first cdf file'
        second_cdf_file_data = b'second cdf file'
        mock_get_cdf_file.side_effect = [
            {"link": "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_sw-particles_20250606_v003.cdf",
             "data": first_cdf_file_data},
            {"link": "http://www.fbi.gov/psp_instrument2_l2-ephem_20181101_v1.27.0.cdf", "data": second_cdf_file_data}
        ]

        mock_cdf_parser.parse_cdf_bytes.side_effect = [
            CdfFileInfo(CdfGlobalInfo("psp_instrument1_l2-summary", "Parker Solar Probe Level 2 Summary", "1.27.0",
                                      date(2022, 11, 12)),
                        [CdfVariableInfo("VAR1", "variable 1", "time-series", None),
                         CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units")]),
            CdfFileInfo(CdfGlobalInfo("psp_instrument2_l2-ephem", "Parker Solar Probe Level 2 Ephemeris", "1.27.0",
                                      date(2022, 11, 13)),
                        [CdfVariableInfo("VAR3", "variable 3", "time-series", None),
                         CdfVariableInfo("VAR4", "variable 4", "spectrogram", None)])
        ]

        actual_index = get_metadata_index()

        mock_get_all_metadata.assert_called_once()

        mock_get_cdf_file.assert_has_calls([
            call("psp_instrument1_l2-summary_20181101_v1.27.0.cdf"),
            call("psp_instrument2_l2-ephem_20181101_v1.27.0.cdf")
        ])

        expected_index = [
            {
                "logical_source": "psp_instrument1_l2-summary",
                "logical_source_description": "Parker Solar Probe Level 2 Summary",
                "version": "1.27.0",
                "dates_available": [["2018-11-01", "2018-11-04"]],
                "variables": [{'catalog_description': 'variable 1',
                               'display_type': 'time-series',
                               'variable_name': 'VAR1', 'units': None},
                              {'catalog_description': 'variable 2',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR2', 'units': "Units"}],
                "source_file_format": "http://wwww.youtube.com/psp_instrument1_l2-summary_%yyyymmdd%_v1.27.0.cdf",
                "description_source_file": 'http://wwww.youtube.com/psp_instrument1_l2-summary_20181101_v1.27.0.cdf',
                "generation_date": "2022-11-12",
                "instrument": "instrument1",
                "mission": "IMAP",
                "file_cadence": "daily",
            },
            {
                "logical_source": "psp_instrument2_l2-ephem",
                "logical_source_description": "Parker Solar Probe Level 2 Ephemeris",
                "version": "1.27.0",
                "dates_available": [["2018-11-01", "2018-11-02"], ["2018-11-04", "2018-11-04"]],
                "variables": [{'catalog_description': 'variable 3',
                               'display_type': 'time-series',
                               'variable_name': 'VAR3', 'units': None},
                              {'catalog_description': 'variable 4',
                               'display_type': 'spectrogram',
                               'variable_name': 'VAR4', 'units': None}],
                "source_file_format": "http://www.fbi.gov/psp_instrument2_l2-ephem_%yyyymmdd%_v1.27.0.cdf",
                "description_source_file": 'http://www.fbi.gov/psp_instrument2_l2-ephem_20181101_v1.27.0.cdf',
                "generation_date": "2022-11-13",
                "instrument": "ISOIS",
                "mission": "IMAP",
                "file_cadence": "daily",
            }
        ]

        self.assertEqual(expected_index, actual_index)

        self.assertEqual(
            [call(first_cdf_file_data, DefaultVariableSelector), call(second_cdf_file_data, DefaultVariableSelector)],
            mock_cdf_parser.parse_cdf_bytes.call_args_list)
