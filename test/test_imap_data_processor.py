from unittest import TestCase
from unittest.mock import patch, Mock, call

from src.cdf_variable_parser import CdfFileInfo
from src.imap_data_processor import group_metadata_by_file_names, get_metadata_index


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

    @patch('src.imap_data_processor.CdfVariableParser')
    @patch('src.imap_data_processor.get_all_metadata')
    @patch('src.imap_data_processor.get_cdf_file')
    def test_get_metadata_index(self, mock_get_cdf_file, mock_get_all_metadata, mock_cdf_variable_parser):
        mock_get_all_metadata.return_value = [
            {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files",
             "file_name": "psp_instrument1_l2-summary_20181101_v1.27.0.cdf",
             "file_root": "psp_instrument1_l2-summary_20181101_v1.27.0.cdf", "file_size": 403371422, "id": 3161,
             "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
             "mod_date": "2022-11-01 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27,
             "timetag": "2018-11-01 00:00:00+00:00", "version": 1},
            {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files",
             "file_name": "psp_instrument1_l2-summary_20181102_v1.27.0.cdf",
             "file_root": "psp_instrument1_l2-summary_20181102_v1.27.0.cdf", "file_size": 403371422, "id": 3161,
             "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
             "mod_date": "2022-11-02 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27,
             "timetag": "2018-11-02 00:00:00+00:00", "version": 1},
            {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files",
             "file_name": "psp_instrument1_l2-summary_20181103_v1.27.0.cdf",
             "file_root": "psp_instrument1_l2-summary_20181103_v1.27.0.cdf", "file_size": 403371422, "id": 3161,
             "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
             "mod_date": "2022-11-03 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27,
             "timetag": "2018-11-03 00:00:00+00:00", "version": 1},
            {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files",
             "file_name": "psp_instrument1_l2-summary_20181104_v1.27.0.cdf",
             "file_root": "psp_instrument1_l2-summary_20181104_v1.27.0.cdf", "file_size": 403371422, "id": 3161,
             "instrument_id": "instrument1", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
             "mod_date": "2022-11-04 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27,
             "timetag": "2018-11-04 00:00:00+00:00", "version": 1},
            {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files",
             "file_name": "psp_instrument2_l2-ephem_20181101_v1.27.0.cdf",
             "file_root": "psp_instrument2_l2-ephem_20181101_v1.27.0.cdf", "file_size": 403371422, "id": 3161,
             "instrument_id": "instrument2", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
             "mod_date": "2022-11-01 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27,
             "timetag": "2018-11-01 00:00:00+00:00", "version": 1},
            {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files",
             "file_name": "psp_instrument2_l2-ephem_20181102_v1.27.0.cdf",
             "file_root": "psp_instrument2_l2-ephem_20181102_v1.27.0.cdf", "file_size": 403371422, "id": 3161,
             "instrument_id": "instrument2", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
             "mod_date": "2022-11-02 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27,
             "timetag": "2018-11-02 00:00:00+00:00", "version": 1},
            {"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files",
             "file_name": "psp_instrument2_l2-ephem_20181104_v1.27.0.cdf",
             "file_root": "psp_instrument2_l2-ephem_20181104_v1.27.0.cdf", "file_size": 403371422, "id": 3161,
             "instrument_id": "instrument2", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
             "mod_date": "2022-11-04 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": True, "revision": 27,
             "timetag": "2018-11-04 00:00:00+00:00", "version": 1}
        ]

        mock_get_cdf_file.side_effect = [
            {"link": "http://wwww.youtube.com/psp_instrument1_l2-summary_20181101_v1.27.0.cdf",
             "data": b'first cdf file'},
            {"link": "http://www.fbi.gov/psp_instrument2_l2-ephem_20181101_v1.27.0.cdf", "data": b'second cdf file'}
        ]

        mock_cdf_variable_parser.parse_info_from_cdf_bytes.side_effect = [
            CdfFileInfo("source1", "1.27.0", {"variable 1 v1.27.0": "VAR1", "variable 2 v1.27.0": "VAR2"}),
            CdfFileInfo("source2", "1.27.0", {"variable 3 v1.27.0": "VAR3", "variable 4 v1.27.0": "VAR4"})
        ]

        actual_index = get_metadata_index()

        mock_get_all_metadata.assert_called_once()

        mock_get_cdf_file.assert_has_calls([
            call("psp_instrument1_l2-summary_20181101_v1.27.0.cdf"),
            call("psp_instrument2_l2-ephem_20181101_v1.27.0.cdf")
        ])

        expected_index = [
            {
                "logical_source": "source1",
                "version": "1.27.0",
                "dates_available": [["2018-11-01", "2018-11-04"]],
                "descriptions": {
                    "variable 1 v1.27.0": "VAR1",
                    "variable 2 v1.27.0": "VAR2"}
                ,
                "source_file_format": "http://wwww.youtube.com/psp_instrument1_l2-summary_%yyyymmdd%_v1.27.0.cdf",
                "description_source_file": 'http://wwww.youtube.com/psp_instrument1_l2-summary_20181101_v1.27.0.cdf'
            },
            {
                "logical_source": "source2",
                "version": "1.27.0",
                "dates_available": [["2018-11-01", "2018-11-02"], ["2018-11-04", "2018-11-04"]],
                "descriptions": {
                    "variable 3 v1.27.0": "VAR3",
                    "variable 4 v1.27.0": "VAR4"
                },
                "source_file_format": "http://www.fbi.gov/psp_instrument2_l2-ephem_%yyyymmdd%_v1.27.0.cdf",
                "description_source_file": 'http://www.fbi.gov/psp_instrument2_l2-ephem_20181101_v1.27.0.cdf'
            }
        ]

        self.assertEqual(expected_index, actual_index)
