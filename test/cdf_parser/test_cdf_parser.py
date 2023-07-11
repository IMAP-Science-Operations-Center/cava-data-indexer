import os
import unittest
from unittest.mock import patch

from data_indexer.cdf_parser.cdf_parser import CdfParser, CdfFileInfo


class TestCdfParser(unittest.TestCase):

    @patch('data_indexer.cdf_parser.cdf_parser.tempfile.TemporaryDirectory')
    @patch('data_indexer.cdf_parser.cdf_parser.CdfGlobalParser')
    @patch('data_indexer.cdf_parser.cdf_parser.CdfVariableParser')
    @patch('data_indexer.cdf_parser.cdf_parser.pycdf')
    def test_parses_global_info_and_variable_info(self, mock_pycdf, mock_variable_parser, mock_global_parser,
                                                  mock_temp_directory):
        mock_temp_directory_name = './test_data_for_cdf_parser/'
        os.makedirs(mock_temp_directory_name, exist_ok=True)
        mock_temp_directory.return_value.__enter__.return_value = mock_temp_directory_name

        mock_cdf = mock_pycdf.CDF.return_value.__enter__.return_value

        expected_temp_file_name = './test_data_for_cdf_parser/temp.cdf'

        self.assertTrue(os.path.isdir(mock_temp_directory_name))
        self.assertFalse(os.path.exists(expected_temp_file_name))
        mock_cdf_bytes = b'cdf bytes'

        output = CdfParser.parse_cdf_bytes(mock_cdf_bytes)

        self.assertIsInstance(output, CdfFileInfo)
        self.assertIs(mock_global_parser.parse_global_variables_from_cdf.return_value, output.global_info)
        self.assertIs(mock_variable_parser.parse_info_from_cdf.return_value, output.variable_infos)

        with open(expected_temp_file_name, 'rb') as f:
            temp_file_contents = f.read()
        self.assertEqual(mock_cdf_bytes, temp_file_contents)
        mock_pycdf.CDF.assert_called_with(expected_temp_file_name)

        mock_global_parser.parse_global_variables_from_cdf.assert_called_with(mock_cdf)
        mock_variable_parser.parse_info_from_cdf.assert_called_with(mock_cdf)

        os.remove(expected_temp_file_name)
        os.removedirs(mock_temp_directory_name)


if __name__ == '__main__':
    unittest.main()
