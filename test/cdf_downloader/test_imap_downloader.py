from unittest import TestCase
from unittest.mock import Mock, patch, call

from data_indexer.cdf_downloader.imap_downloader import get_all_metadata, get_cdf_file


class TestDataDownloader(TestCase):

    @patch('data_indexer.cdf_downloader.imap_downloader.urllib')
    def test_download_metadata_from_mock_server(self, mock_urllib):
        mock_response = Mock()
        mock_response.read.return_value = '[{"absolute_version": 127, "data_level": "l2", "descriptor": "vid", "directory_path": "fake://../cdf_files", "file_name": "psp_isois_l2-summary_20181102_v1.27.0.cdf", "file_root": "psp_isois_l2-summary_20181102_v1.27.0.cdf", "file_size": 403371422, "id": 3161, "instrument_id": "isois", "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5", "mod_date": "2022-11-02 17:21:19+00:00", "mode": "xos1", "pred_rec": "r", "released": true, "revision": 27, "timetag": "2018-11-02 00:00:00+00:00", "version": 1}]'
        mock_urllib.request.urlopen.return_value = mock_response

        response = get_all_metadata()

        expected_response = [{
            "absolute_version": 127,
            "data_level": "l2",
            "descriptor": "vid",
            "directory_path": "fake://../cdf_files",
            "file_name": "psp_isois_l2-summary_20181102_v1.27.0.cdf",
            "file_root": "psp_isois_l2-summary_20181102_v1.27.0.cdf",
            "file_size": 403371422,
            "id": 3161,
            "instrument_id": "isois",
            "md5checksum": "0502a7e4a86e1d78ec7c73515f2dc7d5",
            "mod_date": "2022-11-02 17:21:19+00:00",
            "mode": "xos1",
            "pred_rec": "r",
            "released": True,
            "revision": 27,
            "timetag": "2018-11-02 00:00:00+00:00",
            "version": 1
        }]

        self.assertEqual(expected_response, response)
        mock_urllib.request.urlopen.assert_called_with("http://3.139.73.210/dev/science-files-metadata")

    @patch('data_indexer.cdf_downloader.imap_downloader.urllib')
    def test_download_individual_file(self, mock_urllib):
        mock_science_download_response = Mock()
        mock_science_download_response.read.return_value = b"/science_response"

        mock_file_download_response = Mock()
        mock_file_download_response.read.return_value = b'This is a file'
        mock_urllib.request.urlopen.side_effect = [mock_science_download_response, mock_file_download_response]

        response = get_cdf_file("some cdf file")

        self.assertEqual([call("http://3.139.73.210/dev/science-files-download?file=some cdf file"),
                          call("http://3.139.73.210/science_response")], mock_urllib.request.urlopen.call_args_list)
        self.assertEqual({"link": "http://3.139.73.210/science_response", "data": b'This is a file'}, response)
