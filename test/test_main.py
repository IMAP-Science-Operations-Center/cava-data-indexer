import sys
import unittest
from unittest.mock import patch

from main import main


class TestMain(unittest.TestCase):
    @patch('main.imap_data_processor')
    @patch('main.json.dump')
    @patch('main.open')
    def test_save_metadata_index_imap(self, mock_open, mock_json_dump, mock_data_processor):
        with patch.object(sys, 'argv', ['main.py', 'imap']):
            main()

        mock_json_dump.assert_called_with(mock_data_processor.get_metadata_index.return_value,
                                          mock_open.return_value.__enter__.return_value, indent=2)
        mock_open.assert_called_with('index_imap.v1.json', 'w')
        mock_open.return_value.__exit__.assert_called()

    @patch('main.PspDataProcessor.get_metadata_index')
    @patch('main.json.dump')
    @patch('main.open')
    def test_save_metadata_index_psp(self, mock_open, mock_json_dump, mock_get_metadata_index):
        with patch.object(sys, 'argv', ['main.py', 'psp']):
            main()

        mock_json_dump.assert_called_with(mock_get_metadata_index.return_value,
                                          mock_open.return_value.__enter__.return_value, indent=2)
        mock_open.assert_called_with('index_psp.v1.json', 'w')
        mock_open.return_value.__exit__.assert_called()


if __name__ == '__main__':
    unittest.main()
