import unittest
from pathlib import Path
from unittest.mock import patch, call

from imap_data_access.io import IMAPDataAccessError

from data_indexer.imap_data_access_utility import get_with_retry


class TestImapDataAccessUtility(unittest.TestCase):
    @patch('data_indexer.imap_data_access_utility.time.sleep')
    @patch('data_indexer.imap_data_access_utility.imap_data_access.download')
    def test_retries(self, mock_download, mock_sleep):
        data_product_filename = "imap_swe_sci_20260101_v001.cdf"
        okay_path = Path("")
        mock_download.side_effect = [
            IMAPDataAccessError,
            IMAPDataAccessError,
            IMAPDataAccessError,
            okay_path,
        ]

        result = get_with_retry(data_product_filename)
        self.assertIs(result, okay_path)
        self.assertEqual(mock_download.call_args_list, [call(data_product_filename)]*4)
        self.assertEqual(mock_sleep.call_args_list, [call(1), call(2), call(4)])


    @patch('data_indexer.imap_data_access_utility.time.sleep')
    @patch('data_indexer.imap_data_access_utility.imap_data_access.download')
    def test_retries_n_times(self, mock_download, _):
        data_product_filename = "imap_swe_sci_20260101_v001.cdf"
        okay_path = Path("")
        expected_error = IMAPDataAccessError()
        mock_download.side_effect = [
            IMAPDataAccessError,
            expected_error,
            okay_path,
        ]

        with self.assertRaises(IMAPDataAccessError) as actual_error:
            get_with_retry(data_product_filename, times=2)

        self.assertIs(expected_error, actual_error.exception)
        self.assertEqual(mock_download.call_args_list, [call(data_product_filename)]*2)


if __name__ == '__main__':
    unittest.main()
