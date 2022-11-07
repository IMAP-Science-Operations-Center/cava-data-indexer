import unittest
from unittest.mock import patch

from main import main


class TestMain(unittest.TestCase):
    @patch('main.get_metadata_index')
    @patch('main.json.dump')
    @patch('main.open')
    def test_save_metadata_index(self, mock_open, mock_json_dump, mock_get_metadata_index):
        main()

        mock_json_dump.assert_called_with(mock_get_metadata_index.return_value, mock_open.return_value.__enter__.return_value, indent=2)
        mock_open.assert_called_with('index.json', 'w')
        mock_open.return_value.__exit__.assert_called()


if __name__ == '__main__':
    unittest.main()
