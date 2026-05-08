import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, call, patch

import spacepy.pycdf.const
from spacepy.pycdf import CDFError

from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalInfo
from data_indexer.cdf_parser.cdf_parser import CdfFileInfo
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.imap_data_processor import INSTRUMENTS, L2_L3_DATA_LEVELS, get_metadata_index

MODULE = "data_indexer.imap_data_processor"


class TestImapDataProcessor(TestCase):
    def setUp(self):
        os.environ["IMAP_API_DOWNLOAD_URL"] = "https://api.dev.imap-mission.com/download/"

    @staticmethod
    def _stub_data_product_metadata_query(cdf_metadata_rows: list[dict]):
        def query_chunked_data_product(instrument: str, data_level: str, today: date) -> list[dict]:
            return list(cdf_metadata_rows)

        return query_chunked_data_product

    @patch(f"{MODULE}.CdfParser")
    @patch(f"{MODULE}.query_chunked_data_product")
    @patch(f"{MODULE}.get_with_retry")
    def test_get_metadata_index(self, mock_get_with_retry, mock_query_chunked, mock_cdf_parser):

        l3a_protons_data_product_v3 = (
            "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf"
        )
        l3a_protons_data_product_v2_outdated = (
            "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v002.cdf"
        )

        l3a_pui_data_product_20250606_v3 = (
            "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250606_v003.cdf"
        )
        l3a_pui_data_product_20250607_v2 = (
            "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250607_v002.cdf"
        )

        l3a_pui_diff_instrument_product = (
            "fake-mission/fake-instrument2/l3a/2025/06/fake-mission_fake-instrument2_l3a_pui-he_20250607_v003.cdf"
        )
        l3b_pui_data_product = (
            "fake-mission/fake-instrument/l3b/2025/06/fake-mission_fake-instrument_l3b_pui-he_20250607_v003.cdf"
        )

        query_rows: list[dict] = [
            {
                "file_path": l3a_protons_data_product_v2_outdated,
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": "protons",
                "start_date": "20250606",
                "repointing": None,
                "version": "v002",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:59",
            },
            {
                "file_path": l3a_protons_data_product_v3,
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": "protons",
                "start_date": "20250606",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:58",
            },
            {
                "file_path": l3a_pui_data_product_20250607_v2,
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": "pui-he",
                "start_date": "20250607",
                "repointing": None,
                "version": "v002",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:12:40",
            },
            {
                "file_path": l3a_pui_data_product_20250606_v3,
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": "pui-he",
                "start_date": "20250606",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:59",
            },
            {
                "file_path": l3a_pui_diff_instrument_product,
                "instrument": "fake-instrument2",
                "data_level": "l3a",
                "descriptor": "pui-he",
                "start_date": "20250607",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:12:40",
            },
            {
                "file_path": l3b_pui_data_product,
                "instrument": "fake-instrument",
                "data_level": "l3b",
                "descriptor": "pui-he",
                "start_date": "20250607",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:12:40",
            },
        ]
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query(query_rows)

        first_cdf_response = Mock()
        second_cdf_response = Mock()
        third_cdf_response = Mock()
        fourth_cdf_response = Mock()

        mock_get_with_retry.side_effect = [
            first_cdf_response,
            second_cdf_response,
            third_cdf_response,
            fourth_cdf_response,
        ]

        mock_cdf_parser.parse_cdf.side_effect = [
            CdfFileInfo(
                CdfGlobalInfo(
                    "fake-mission_fake-instrument_l3a_protons",
                    "Parker Solar Probe Level 2 Summary",
                    "1.27.0",
                    date(2022, 11, 12),
                ),
                [
                    CdfVariableInfo("VAR1", "variable 1", "time-series", None, "axis_label_1"),
                    CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label_2"),
                ],
            ),
            CdfFileInfo(
                CdfGlobalInfo(
                    "fake-mission_fake-instrument_l3a_pui-he",
                    "Parker Solar Probe Level 2 Ephemeris",
                    "1.27.0",
                    date(2022, 11, 13),
                ),
                [
                    CdfVariableInfo("VAR3", "variable 3", "time-series", None, "axis_label_3"),
                    CdfVariableInfo("VAR4", "variable 4", "spectrogram", None, "axis_label_4"),
                ],
            ),
            CdfFileInfo(
                CdfGlobalInfo(
                    "fake-mission_fake-instrument2_l3a_pui-he",
                    "Parker Solar Probe Level 2 Summary",
                    "1.27.0",
                    date(2022, 11, 12),
                ),
                [
                    CdfVariableInfo("VAR1", "variable 1", "time-series", None, "axis_label_1"),
                    CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label_2"),
                ],
            ),
            CdfFileInfo(
                CdfGlobalInfo(
                    "fake-mission_fake-instrument_l3b_pui-he",
                    "Parker Solar Probe Level 2 Ephemeris",
                    "1.27.0",
                    date(2022, 11, 13),
                ),
                [
                    CdfVariableInfo("VAR3", "variable 3", "time-series", None, "axis_label_3"),
                    CdfVariableInfo("VAR4", "variable 4", "spectrogram", None, "axis_label_4"),
                ],
            ),
        ]

        actual_index = get_metadata_index()

        l3a_protons_data_product_v3_url = f"https://api.dev.imap-mission.com/download/{l3a_protons_data_product_v3}"
        l3a_pui_data_product_20250606_v3_url = (
            f"https://api.dev.imap-mission.com/download/{l3a_pui_data_product_20250606_v3}"
        )
        l3a_pui_data_product_20250607_v2_url = (
            f"https://api.dev.imap-mission.com/download/{l3a_pui_data_product_20250607_v2}"
        )
        l3a_pui_diff_instrument_product_url = (
            f"https://api.dev.imap-mission.com/download/{l3a_pui_diff_instrument_product}"
        )
        l3b_pui_data_product_url = f"https://api.dev.imap-mission.com/download/{l3b_pui_data_product}"

        mock_get_with_retry.assert_has_calls(
            [
                call(l3a_protons_data_product_v3),
                call(l3a_pui_data_product_20250607_v2),
                call(l3a_pui_diff_instrument_product),
                call(l3b_pui_data_product),
            ]
        )

        expected_index = [
            {
                "file_timeranges": [
                    {
                        "start_time": "2025-06-06T00:00:00+00:00",
                        "end_time": "2025-06-07T00:00:00+00:00",
                        "url": l3a_protons_data_product_v3_url,
                    }
                ],
                "logical_source": "fake-mission_fake-instrument_l3a_protons",
                "logical_source_description": "Parker Solar Probe Level 2 Summary",
                "data_level": "l3a",
                "variables": [
                    {
                        "catalog_description": "variable 1",
                        "display_type": "time-series",
                        "variable_name": "VAR1",
                        "units": None,
                        "axis_label": "axis_label_1",
                    },
                    {
                        "catalog_description": "variable 2",
                        "display_type": "spectrogram",
                        "variable_name": "VAR2",
                        "units": "Units",
                        "axis_label": "axis_label_2",
                    },
                ],
                "generation_date": "2022-11-12",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
                "version": "",
            },
            {
                "file_timeranges": [
                    {
                        "start_time": "2025-06-06T00:00:00+00:00",
                        "end_time": "2025-06-07T00:00:00+00:00",
                        "url": l3a_pui_data_product_20250606_v3_url,
                    },
                    {
                        "start_time": "2025-06-07T00:00:00+00:00",
                        "end_time": "2025-06-08T00:00:00+00:00",
                        "url": l3a_pui_data_product_20250607_v2_url,
                    },
                ],
                "logical_source": "fake-mission_fake-instrument_l3a_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Ephemeris",
                "data_level": "l3a",
                "variables": [
                    {
                        "catalog_description": "variable 3",
                        "display_type": "time-series",
                        "variable_name": "VAR3",
                        "units": None,
                        "axis_label": "axis_label_3",
                    },
                    {
                        "catalog_description": "variable 4",
                        "display_type": "spectrogram",
                        "variable_name": "VAR4",
                        "units": None,
                        "axis_label": "axis_label_4",
                    },
                ],
                "generation_date": "2022-11-13",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
                "version": "",
            },
            {
                "file_timeranges": [
                    {
                        "start_time": "2025-06-07T00:00:00+00:00",
                        "end_time": "2025-06-08T00:00:00+00:00",
                        "url": l3a_pui_diff_instrument_product_url,
                    },
                ],
                "logical_source": "fake-mission_fake-instrument2_l3a_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Summary",
                "data_level": "l3a",
                "variables": [
                    {
                        "catalog_description": "variable 1",
                        "display_type": "time-series",
                        "variable_name": "VAR1",
                        "units": None,
                        "axis_label": "axis_label_1",
                    },
                    {
                        "catalog_description": "variable 2",
                        "display_type": "spectrogram",
                        "variable_name": "VAR2",
                        "units": "Units",
                        "axis_label": "axis_label_2",
                    },
                ],
                "generation_date": "2022-11-12",
                "instrument": "fake-instrument2",
                "mission": "IMAP",
                "file_cadence": "daily",
                "version": "",
            },
            {
                "file_timeranges": [
                    {
                        "start_time": "2025-06-07T00:00:00+00:00",
                        "end_time": "2025-06-08T00:00:00+00:00",
                        "url": l3b_pui_data_product_url,
                    },
                ],
                "logical_source": "fake-mission_fake-instrument_l3b_pui-he",
                "logical_source_description": "Parker Solar Probe Level 2 Ephemeris",
                "data_level": "l3b",
                "variables": [
                    {
                        "catalog_description": "variable 3",
                        "display_type": "time-series",
                        "variable_name": "VAR3",
                        "units": None,
                        "axis_label": "axis_label_3",
                    },
                    {
                        "catalog_description": "variable 4",
                        "display_type": "spectrogram",
                        "variable_name": "VAR4",
                        "units": None,
                        "axis_label": "axis_label_4",
                    },
                ],
                "generation_date": "2022-11-13",
                "instrument": "fake-instrument",
                "mission": "IMAP",
                "file_cadence": "daily",
                "version": "",
            },
        ]

        self.assertEqual(expected_index, actual_index)

        self.assertEqual(
            [
                call(first_cdf_response, DefaultVariableSelector),
                call(second_cdf_response, DefaultVariableSelector),
                call(third_cdf_response, DefaultVariableSelector),
                call(fourth_cdf_response, DefaultVariableSelector),
            ],
            mock_cdf_parser.parse_cdf.call_args_list,
        )

    @patch(f"{MODULE}.datetime")
    @patch(f"{MODULE}.query_chunked_data_product")
    def test_calls_chunked_helper_for_mag_l1d_then_full_l2_l3_grid_in_order(
        self, mock_query_chunked, mock_datetime
    ):
        mock_datetime.now.return_value = datetime(2026, 5, 8, tzinfo=timezone.utc)
        today = date(2026, 5, 8)
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query([])

        get_metadata_index()

        expected_calls = [call(instrument="mag", data_level="l1d", today=today)]
        for instrument in INSTRUMENTS:
            for data_level in L2_L3_DATA_LEVELS:
                expected_calls.append(call(instrument=instrument, data_level=data_level, today=today))
        self.assertEqual(mock_query_chunked.call_args_list, expected_calls)
        self.assertEqual(mock_query_chunked.call_count, 1 + len(INSTRUMENTS) * len(L2_L3_DATA_LEVELS))

    @patch(f"{MODULE}.CdfParser")
    @patch(f"{MODULE}.query_chunked_data_product")
    @patch(f"{MODULE}.get_with_retry")
    def test_invokes_get_with_retry_for_every_data_product_returned_by_chunked_helper(
        self, mock_get_with_retry, mock_query_chunked, _mock_cdf_parser
    ):
        response_by_pair: dict[tuple[str, str], list[dict]] = {
            ("mag", "l1d"): [self.create_cdf_metadata_query_response("l1d")],
        }
        for data_level in L2_L3_DATA_LEVELS:
            response_by_pair[("codice", data_level)] = [self.create_cdf_metadata_query_response(data_level)]

        def query_chunked_data_product(instrument: str, data_level: str, today: date) -> list[dict]:
            return response_by_pair.get((instrument, data_level), [])

        mock_query_chunked.side_effect = query_chunked_data_product
        mock_get_with_retry.side_effect = [Path(rows[0]["file_path"]) for rows in response_by_pair.values()]

        get_metadata_index()

        mock_get_with_retry.assert_has_calls(
            [call(rows[0]["file_path"]) for rows in response_by_pair.values()]
        )
        self.assertEqual(mock_get_with_retry.call_count, len(response_by_pair))

    @patch(f"{MODULE}.CdfParser")
    @patch(f"{MODULE}.query_chunked_data_product")
    @patch(f"{MODULE}.get_with_retry")
    def test_get_metadata_index_returns_instrument_names_correctly(
        self, _mock_get_with_retry, mock_query_chunked, _
    ):
        lowercase_instruments = [
            "codice",
            "glows",
            "hi",
            "hit",
            "idex",
            "lo",
            "mag",
            "swapi",
            "swe",
            "ultra",
        ]
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
        query_rows: list[dict] = [
            {
                "file_path": "",
                "instrument": instrument,
                "data_level": "l3a",
                "descriptor": "protons-3mo",
                "start_date": "20250606",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:58",
            }
            for instrument in lowercase_instruments
        ]
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query(query_rows)

        actual_index = get_metadata_index()

        self.assertEqual(10, len(actual_index))

        for expected, actual in zip(uppercase_instruments, actual_index):
            self.assertEqual(expected, actual["instrument"])

    @patch(f"{MODULE}.query_chunked_data_product")
    def test_get_metadata_index_excludes_log_files_and_files_with_uuids(self, mock_query_chunked):
        query_rows: list[dict] = [
            {
                "file_path": (
                    "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v002.cdf"
                ),
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": "this is a log file",
                "start_date": "20250606",
                "repointing": None,
                "version": "v002",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:59",
            },
            {
                "file_path": (
                    "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf"
                ),
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": f"the id is {uuid.uuid4()}",
                "start_date": "20250606",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:58",
            },
        ]
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query(query_rows)

        actual_index = get_metadata_index()

        mock_query_chunked.assert_called()
        self.assertEqual([], actual_index)

    @patch(f"{MODULE}.CdfParser")
    @patch(f"{MODULE}.query_chunked_data_product")
    @patch(f"{MODULE}.get_with_retry")
    def test_ignores_poorly_formatted_cdfs(self, _, mock_query_chunked, mock_cdf_parser):
        expected_file_path_1 = (
            "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_protons_20250606_v003.cdf"
        )
        expected_file_path_2 = (
            "fake-mission/fake-instrument/l3a/2025/06/fake-mission_fake-instrument_l3a_pui-he_20250606_v003.cdf"
        )

        query_rows: list[dict] = [
            {
                "file_path": expected_file_path_1,
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": "protons",
                "start_date": "20250606",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:58",
            },
            {
                "file_path": expected_file_path_2,
                "instrument": "fake-instrument",
                "data_level": "l3a",
                "descriptor": "pui-he",
                "start_date": "20250606",
                "repointing": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:59",
            },
        ]
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query(query_rows)

        mock_cdf_parser.parse_cdf.side_effect = [
            CDFError(spacepy.pycdf.const.NOT_A_CDF_OR_NOT_SUPPORTED),
            CdfFileInfo(
                CdfGlobalInfo(
                    "fake-mission_fake-instrument_l3a_protons",
                    "Parker Solar Probe Level 2 Summary",
                    "1.27.0",
                    date(2022, 11, 12),
                ),
                [
                    CdfVariableInfo("VAR1", "variable 1", "time-series", None, "axis"),
                    CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "Axis"),
                ],
            ),
        ]
        actual_index = get_metadata_index()

        self.assertEqual(1, len(actual_index))

    @patch(f"{MODULE}.CdfParser")
    @patch(f"{MODULE}.query_chunked_data_product")
    @patch(f"{MODULE}.get_with_retry")
    def test_handles_files_with_carrington_cadence(self, _, mock_query_chunked, mock_cdf_parser):
        glows_l3b_file_path = "some/path/on/server/imap_glows_l3b_glows-descriptor_20250101_v000.cdf"
        glows_l3c_file_path = "some/path/on/server/imap_glows_l3c_glows-descriptor_20250101_v001.cdf"
        glows_l3d_file_path = "some/path/on/server/imap_glows_l3d_glows-descriptor_19470101-cr2292_v000.cdf"

        query_rows: list[dict] = [
            {
                "file_path": glows_l3b_file_path,
                "instrument": "glows",
                "data_level": "l3b",
                "descriptor": "glows-descriptor",
                "start_date": "20250101",
                "repointing": None,
                "cr": None,
                "version": "v000",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:58",
            },
            {
                "file_path": glows_l3c_file_path,
                "instrument": "glows",
                "data_level": "l3c",
                "descriptor": "glows-descriptor",
                "start_date": "20250101",
                "repointing": None,
                "cr": None,
                "version": "v001",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:58",
            },
            {
                "file_path": glows_l3d_file_path,
                "instrument": "glows",
                "data_level": "l3d",
                "descriptor": "glows-descriptor",
                "start_date": "19470101",
                "repointing": None,
                "cr": 2292,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:59",
            },
        ]
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query(query_rows)

        mock_cdf_parser.parse_cdf.side_effect = [
            CdfFileInfo(
                CdfGlobalInfo(
                    "imap_glows_l3b_glows-descriptor", "imap glows l3b glows-descriptor", "v000", date(2022, 11, 12)
                ),
                [CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label")],
            ),
            CdfFileInfo(
                CdfGlobalInfo(
                    "imap_glows_l3c_glows-descriptor", "imap glows l3c glows-descriptor", "v001", date(2022, 11, 12)
                ),
                [CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label")],
            ),
            CdfFileInfo(
                CdfGlobalInfo(
                    "imap_glows_l3d_glows-descriptor", "imap glows l3d glows-descriptor", "v000", date(2022, 11, 12)
                ),
                [CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label")],
            ),
        ]

        expected_start_time = "2024-12-10T12:59:28.319992+00:00"
        expected_end_time = "2025-01-06T19:35:54.239992+00:00"
        expected_index = [
            {
                "file_cadence": "carrington_rotation",
                "file_timeranges": [
                    {
                        "end_time": "2025-01-06T19:35:54.239992+00:00",
                        "start_time": expected_start_time,
                        "url": "https://api.dev.imap-mission.com/download/some/path/on/server/imap_glows_l3b_glows-descriptor_20250101_v000.cdf",
                    }
                ],
                "generation_date": "2022-11-12",
                "instrument": "GLOWS",
                "logical_source": "imap_glows_l3b_glows-descriptor",
                "logical_source_description": "imap glows l3b glows-descriptor",
                "data_level": "l3b",
                "mission": "IMAP",
                "version": "",
                "variables": [
                    {
                        "catalog_description": "variable 2",
                        "display_type": "spectrogram",
                        "units": "Units",
                        "variable_name": "VAR2",
                        "axis_label": "axis_label",
                    }
                ],
            },
            {
                "file_cadence": "carrington_rotation",
                "file_timeranges": [
                    {
                        "end_time": expected_end_time,
                        "start_time": expected_start_time,
                        "url": "https://api.dev.imap-mission.com/download/some/path/on/server/imap_glows_l3c_glows-descriptor_20250101_v001.cdf",
                    }
                ],
                "generation_date": "2022-11-12",
                "instrument": "GLOWS",
                "logical_source": "imap_glows_l3c_glows-descriptor",
                "logical_source_description": "imap glows l3c glows-descriptor",
                "data_level": "l3c",
                "mission": "IMAP",
                "version": "",
                "variables": [
                    {
                        "catalog_description": "variable 2",
                        "display_type": "spectrogram",
                        "units": "Units",
                        "variable_name": "VAR2",
                        "axis_label": "axis_label",
                    }
                ],
            },
            {
                "file_cadence": "carrington_rotation",
                "file_timeranges": [
                    {
                        "end_time": "2025-01-06T19:35:54.239992+00:00",
                        "start_time": expected_start_time,
                        "url": "https://api.dev.imap-mission.com/download/some/path/on/server/imap_glows_l3d_glows-descriptor_19470101-cr2292_v000.cdf",
                    }
                ],
                "generation_date": "2022-11-12",
                "instrument": "GLOWS",
                "logical_source": "imap_glows_l3d_glows-descriptor",
                "logical_source_description": "imap glows l3d glows-descriptor",
                "data_level": "l3d",
                "mission": "IMAP",
                "version": "",
                "variables": [
                    {
                        "catalog_description": "variable 2",
                        "display_type": "spectrogram",
                        "units": "Units",
                        "variable_name": "VAR2",
                        "axis_label": "axis_label",
                    }
                ],
            },
        ]

        self.assertEqual(expected_index, get_metadata_index())

    @patch(f"{MODULE}.CdfParser")
    @patch(f"{MODULE}.query_chunked_data_product")
    @patch(f"{MODULE}.get_with_retry")
    def test_handles_map_cadences(self, _, mock_query_chunked, mock_cdf_parser):
        hi_3month_file_path = "some/path/on/server/imap_hi_l3_intensity-3mo_20250101_v000.cdf"
        hi_6month_file_path = "some/path/on/server/imap_hi_l3_intensity-6mo_20250101_v000.cdf"
        map_with_bad_cadence = "some/path/on/server/imap_hi_l3_intensity-3yr_20250101_v000.cdf"

        query_rows: list[dict] = [
            {
                "file_path": hi_3month_file_path,
                "instrument": "hi",
                "data_level": "l3",
                "descriptor": "intensity-3mo",
                "start_date": "20250101",
                "repointing": None,
                "cr": None,
                "version": "v000",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:58",
            },
            {
                "file_path": hi_6month_file_path,
                "instrument": "hi",
                "data_level": "l3",
                "descriptor": "intensity-6mo",
                "start_date": "20250101",
                "repointing": None,
                "cr": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:59",
            },
            {
                "file_path": map_with_bad_cadence,
                "instrument": "hi",
                "data_level": "l3",
                "descriptor": "intensity-3yr",
                "start_date": "20250101",
                "repointing": None,
                "cr": None,
                "version": "v003",
                "extension": "cdf",
                "ingestion_date": "2024-11-21 21:09:59",
            },
        ]
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query(query_rows)

        mock_cdf_parser.parse_cdf.side_effect = [
            CdfFileInfo(
                CdfGlobalInfo("imap_hi_l3_intensity-3mo", "imap hi l3 intensity-3mo", "v000", date(2022, 11, 12)),
                [CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label")],
            ),
            CdfFileInfo(
                CdfGlobalInfo("imap_hi_l3_intensity-6mo", "imap hi l3 intensity-6mo", "v000", date(2022, 11, 12)),
                [CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label")],
            ),
            CdfFileInfo(
                CdfGlobalInfo("imap_hi_l3_intensity-3yr", "imap hi l3 intensity-3yr", "v000", date(2022, 11, 12)),
                [CdfVariableInfo("VAR2", "variable 2", "spectrogram", "Units", "axis_label")],
            ),
        ]

        expected_start_time = "2025-01-01T00:00:00+00:00"
        expected_3mo_end_time = "2025-04-02T07:30:00+00:00"
        expected_6mo_end_time = "2025-07-02T15:00:00+00:00"
        expected_index = [
            {
                "file_cadence": "three_month_map",
                "file_timeranges": [
                    {
                        "end_time": expected_3mo_end_time,
                        "start_time": expected_start_time,
                        "url": f"https://api.dev.imap-mission.com/download/{hi_3month_file_path}",
                    }
                ],
                "generation_date": "2022-11-12",
                "instrument": "IMAP-Hi",
                "logical_source": "imap_hi_l3_intensity-3mo",
                "logical_source_description": "imap hi l3 intensity-3mo",
                "data_level": "l3",
                "mission": "IMAP",
                "version": "",
                "variables": [
                    {
                        "catalog_description": "variable 2",
                        "display_type": "spectrogram",
                        "units": "Units",
                        "variable_name": "VAR2",
                        "axis_label": "axis_label",
                    }
                ],
            },
            {
                "file_cadence": "six_month_map",
                "file_timeranges": [
                    {
                        "end_time": expected_6mo_end_time,
                        "start_time": expected_start_time,
                        "url": f"https://api.dev.imap-mission.com/download/{hi_6month_file_path}",
                    }
                ],
                "generation_date": "2022-11-12",
                "instrument": "IMAP-Hi",
                "logical_source": "imap_hi_l3_intensity-6mo",
                "logical_source_description": "imap hi l3 intensity-6mo",
                "data_level": "l3",
                "mission": "IMAP",
                "version": "",
                "variables": [
                    {
                        "catalog_description": "variable 2",
                        "display_type": "spectrogram",
                        "units": "Units",
                        "variable_name": "VAR2",
                        "axis_label": "axis_label",
                    }
                ],
            },
        ]

        self.assertEqual(expected_index, get_metadata_index())

    @patch(f"{MODULE}.CdfParser")
    @patch(f"{MODULE}.get_with_retry")
    @patch(f"{MODULE}.query_chunked_data_product")
    def test_returns_empty_index_when_every_chunked_query_is_empty(
        self, mock_query_chunked, mock_get_with_retry, mock_cdf_parser
    ):
        mock_query_chunked.side_effect = self._stub_data_product_metadata_query([])

        actual_index = get_metadata_index()

        self.assertEqual([], actual_index)
        self.assertEqual(0, mock_get_with_retry.call_count)
        self.assertEqual(0, mock_cdf_parser.parse_cdf.call_count)

    def create_cdf_metadata_query_response(
        self,
        data_level,
    ) -> dict[str, str | None]:
        random_file_path = uuid.uuid4()
        return {
            "file_path": f"fake-mission/fake-instrument/{data_level}/2025/06/{random_file_path}.cdf",
            "instrument": "fake-instrument",
            "data_level": data_level,
            "descriptor": "protons",
            "start_date": "20250606",
            "repointing": None,
            "version": "v002",
            "extension": "cdf",
            "ingestion_date": "2024-11-21 21:09:59",
        }
