from unittest import TestCase
from unittest.mock import patch, Mock, call, MagicMock

from src.data_processor import group_metadata_by_file_names, _parse_variables_from_cdf, parse_variables_from_cdf_bytes, \
    get_metadata_index


class TestDataProcessor(TestCase):
    def test_group_metadata_by_file_names(self):
        metadata_summary_1102 = {"file_name": "psp_isois_l2-summary_20181102_v1.27.0.cdf", "timetag":  "2018-11-02 00:00:00+00:00"}
        metadata_summary_1103 = {"file_name": "psp_isois_l2-summary_20181103_v1.27.0.cdf", "timetag":  "2018-11-03 00:00:00+00:00"}
        metadata_ephem_1102 = {"file_name": "psp_isois_l2-ephem_20181102_v1.27.0.cdf", "timetag":  "2018-11-02 00:00:00+00:00"}
        metadata_imap = {"file_name": "imap_super100000000_ql_20210613T035100_0000_xos1_vid_r_v01-01.cdf", "timetag": "2021-06-13 00:00:00+00:00"}
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
        self.assertEqual(expected_output,actual_output)

    def test_parse_variables_from_cdf_returns_list_of_descriptions(self):
        expected_descriptions = [
            "Angle between TPS and Sun, 0 in encounter",
            "Angle between nominal ram and actual ram, 0 in encounter",
            "angle of off-pointing from ecliptic north when not in encounter",
            "LET1A look angle with nominal parker spiral",
            "LET2C look angle with nominal parker spiral",
            "HETA look angle with nominal parker spiral",
            "Lo look angle with nominal parker spiral",
            "Heliocentric distance",
            "HCI latitude",
            "HCI longitude",
            "Heliocentric distance",
            "HGC latitude",
            "HGC longitude",
            "Spacecraft is umbra pointing",
            "Spacecraft is ram pointing"
        ]

        cdf_path = './test_data/test.cdf'
        descriptions = _parse_variables_from_cdf(cdf_path)

        self.assertEqual(expected_descriptions, descriptions)


    def test_parse_variables_from_cdf_bytes_returns_list_of_descriptions(self):
        expected_descriptions = [
            "Angle between TPS and Sun, 0 in encounter",
            "Angle between nominal ram and actual ram, 0 in encounter",
            "angle of off-pointing from ecliptic north when not in encounter",
            "LET1A look angle with nominal parker spiral",
            "LET2C look angle with nominal parker spiral",
            "HETA look angle with nominal parker spiral",
            "Lo look angle with nominal parker spiral",
            "Heliocentric distance",
            "HCI latitude",
            "HCI longitude",
            "Heliocentric distance",
            "HGC latitude",
            "HGC longitude",
            "Spacecraft is umbra pointing",
            "Spacecraft is ram pointing"
        ]

        cdf_path = './test_data/test.cdf'
        with open(cdf_path, 'rb') as file:
            cdf_bytes = file.read()

        descriptions = parse_variables_from_cdf_bytes(cdf_bytes)

        self.assertEqual(expected_descriptions, descriptions)

    @patch('src.data_processor.pycdf')
    @patch('src.data_processor.get_all_metadata')
    @patch('src.data_processor.get_cdf_file')
    def test_get_metadata_index(self, mock_get_cdf_file, mock_get_all_metadata, mock_pycdf):
        mock_get_all_metadata.return_value = [
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument1_l2-summary_20181101_v1.27.0.cdf", "file_root": "psp_instrument1_l2-summary_20181101_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-01 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-01 00:00:00+00:00", "version": 1},
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument1_l2-summary_20181102_v1.27.0.cdf", "file_root": "psp_instrument1_l2-summary_20181102_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-02 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-02 00:00:00+00:00", "version": 1},
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument1_l2-summary_20181103_v1.27.0.cdf", "file_root": "psp_instrument1_l2-summary_20181103_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-03 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-03 00:00:00+00:00", "version": 1},
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument1_l2-summary_20181104_v1.27.0.cdf", "file_root": "psp_instrument1_l2-summary_20181104_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-04 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-04 00:00:00+00:00", "version": 1},
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument2_l2-ephem_20181101_v1.27.0.cdf", "file_root": "psp_instrument2_l2-ephem_20181101_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument2", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-01 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-01 00:00:00+00:00", "version": 1},
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument2_l2-ephem_20181102_v1.27.0.cdf", "file_root": "psp_instrument2_l2-ephem_20181102_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument2", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-02 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-02 00:00:00+00:00", "version": 1},
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument2_l2-ephem_20181103_v1.27.0.cdf", "file_root": "psp_instrument2_l2-ephem_20181103_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument2", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-03 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-03 00:00:00+00:00", "version": 1},
                {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_instrument2_l2-ephem_20181104_v1.27.0.cdf", "file_root": "psp_instrument2_l2-ephem_20181104_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "instrument2", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-04 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27, "timetag": "2018-11-04 00:00:00+00:00", "version": 1}
             ]

        mock_get_cdf_file.side_effect = [
            b'first cdf file',
            b'second cdf file'
        ]

        mock_var_1 = Mock()
        mock_var_1.attrs = {"CATDESC": "variable 1", "VAR_TYPE": "data"}
        mock_var_2 = Mock()
        mock_var_2.attrs = {"CATDESC": "variable 2", "VAR_TYPE": "data"}
        mock_var_3 = Mock()
        mock_var_3.attrs = {"CATDESC": "variable 3", "VAR_TYPE": "data"}
        mock_var_4 = Mock()
        mock_var_4.attrs = {"CATDESC": "variable 4", "VAR_TYPE": "data"}

        mock_pycdf.CDF.return_value.values.side_effect = [
            [mock_var_1, mock_var_2],
            [mock_var_3, mock_var_4]
        ]

        actual_index = get_metadata_index()

        mock_get_all_metadata.assert_called_once()

        mock_get_cdf_file.assert_has_calls([
            call("psp_instrument1_l2-summary_20181101_v1.27.0.cdf"),
            call("psp_instrument2_l2-ephem_20181101_v1.27.0.cdf")
        ])

        expected_index = [
        {
            "descriptions": [
                "variable 1",
                "variable 2"
            ],
            "source_file_format": "psp_instrument1_l2-summary_%yyyymmdd%_v1.27.0.cdf"
        },
        {
            "descriptions": [
                "variable 3",
                "variable 4"
            ],
            "source_file_format": "psp_instrument2_l2-ephem_%yyyymmdd%_v1.27.0.cdf"
        }
        ]

        self.assertEqual(actual_index, expected_index)

