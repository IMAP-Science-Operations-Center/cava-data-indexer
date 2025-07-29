import unittest
from unittest.mock import patch, call

import httpx

from data_indexer.http_client import get_with_retry


class TestHttpClient(unittest.TestCase):
    @patch('data_indexer.http_client.time.sleep')
    @patch('data_indexer.http_client.http_client.get')
    def test_retries(self, mock_get, mock_sleep):
        url = "http://example.com"
        okay_response = httpx.Response(200)
        mock_get.side_effect = [
            httpx.ConnectError("ack"),
            httpx.ConnectTimeout("timed out"),
            httpx.RemoteProtocolError("no response"),
            okay_response,
        ]

        result = get_with_retry(url)
        self.assertIs(result, okay_response)
        self.assertEqual(mock_get.call_args_list, [call(url, follow_redirects=True)]*4)
        self.assertEqual(mock_sleep.call_args_list, [call(1), call(2), call(4)])


    @patch('data_indexer.http_client.time.sleep')
    @patch('data_indexer.http_client.http_client.get')
    def test_retries_n_times(self, mock_get, _):
        url = "http://example.com"
        okay_response = httpx.Response(200)
        expected_error = httpx.RemoteProtocolError("second no response")
        mock_get.side_effect = [
            httpx.RemoteProtocolError("no response"),
            expected_error,
            okay_response,
        ]

        with self.assertRaises(httpx.RemoteProtocolError) as actual_error:
            get_with_retry(url, times=2)

        self.assertIs(expected_error, actual_error.exception)
        self.assertEqual(mock_get.call_args_list, [call(url, follow_redirects=True)]*2)


if __name__ == '__main__':
    unittest.main()
