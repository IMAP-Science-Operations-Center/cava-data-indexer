import unittest
from datetime import date
from pathlib import Path
from unittest.mock import call, patch

from imap_data_access.io import IMAPDataAccessError

from data_indexer.imap_data_access_utility import get_with_retry, query_chunked_data_product

MODULE = "data_indexer.imap_data_access_utility"


class TestImapDataAccessUtility(unittest.TestCase):
    @patch(f"{MODULE}.time.sleep")
    @patch(f"{MODULE}.imap_data_access.download")
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
        self.assertEqual(mock_download.call_args_list, [call(data_product_filename)] * 4)
        self.assertEqual(mock_sleep.call_args_list, [call(1), call(2), call(4)])

    @patch(f"{MODULE}.time.sleep")
    @patch(f"{MODULE}.imap_data_access.download")
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
        self.assertEqual(mock_download.call_args_list, [call(data_product_filename)] * 2)


class TestQueryChunkedDataProduct(unittest.TestCase):
    @patch(f"{MODULE}.imap_data_access.query")
    def test_returns_yearly_results_concatenated_when_every_year_succeeds(self, mock_query):
        mock_query.side_effect = [[{"y": 2024}], [{"y": 2025}], [{"y": 2026}]]

        result = query_chunked_data_product(instrument="mag", data_level="l1d", today=date(2026, 5, 8))

        self.assertEqual(
            mock_query.call_args_list,
            [
                call(instrument="mag", data_level="l1d", ingestion_start_date="20240101", ingestion_end_date="20241231", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20250101", ingestion_end_date="20251231", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20260101", ingestion_end_date="20261231", version="latest"),
            ],
        )
        self.assertEqual(result, [{"y": 2024}, {"y": 2025}, {"y": 2026}])

    @patch(f"{MODULE}.imap_data_access.query")
    def test_bisects_a_year_into_two_halves_when_the_yearly_query_raises_imap_data_access_error(
        self, mock_query
    ):
        responses_by_range: dict[tuple[str, str], list[dict]] = {
            ("20240101", "20241231"): [],
            ("20250101", "20251231"): [],
            ("20260101", "20260702"): [{"half": 1}],
            ("20260703", "20261231"): [{"half": 2}],
        }
        errors_by_range: set[tuple[str, str]] = {("20260101", "20261231")}

        def side_effect(*, ingestion_start_date, ingestion_end_date, **_kwargs):
            key = (ingestion_start_date, ingestion_end_date)
            if key in errors_by_range:
                raise IMAPDataAccessError("500 Internal Server Error")
            return responses_by_range[key]

        mock_query.side_effect = side_effect

        result = query_chunked_data_product(instrument="mag", data_level="l1d", today=date(2026, 5, 8))

        self.assertEqual(
            mock_query.call_args_list,
            [
                call(instrument="mag", data_level="l1d", ingestion_start_date="20240101", ingestion_end_date="20241231", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20250101", ingestion_end_date="20251231", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20260101", ingestion_end_date="20261231", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20260101", ingestion_end_date="20260702", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20260703", ingestion_end_date="20261231", version="latest"),
            ],
        )
        self.assertEqual(result, [{"half": 1}, {"half": 2}])

    @patch(f"{MODULE}.imap_data_access.query")
    def test_bisects_recursively_until_a_sub_range_fits(self, mock_query):
        responses_by_range: dict[tuple[str, str], list[dict]] = {
            ("20240101", "20241231"): [],
            ("20250101", "20251231"): [],
            ("20260101", "20260402"): [{"q": 1}],
            ("20260403", "20260702"): [{"q": 2}],
            ("20260703", "20261231"): [{"h": 2}],
        }
        errors_by_range: set[tuple[str, str]] = {
            ("20260101", "20261231"),
            ("20260101", "20260702"),
        }

        def side_effect(*, ingestion_start_date, ingestion_end_date, **kwargs):
            key = (ingestion_start_date, ingestion_end_date)
            if key in errors_by_range:
                raise IMAPDataAccessError("500 Internal Server Error")
            return responses_by_range[key]

        mock_query.side_effect = side_effect

        result = query_chunked_data_product(instrument="mag", data_level="l1d", today=date(2026, 5, 8))

        self.assertEqual(mock_query.call_count, 7)
        self.assertEqual(result, [{"q": 1}, {"q": 2}, {"h": 2}])

    @patch(f"{MODULE}.imap_data_access.query")
    def test_propagates_imap_data_access_error_when_a_sub_30_day_range_still_fails(self, mock_query):
        mock_query.side_effect = IMAPDataAccessError("500 Internal Server Error")

        with self.assertRaises(IMAPDataAccessError):
            query_chunked_data_product(instrument="mag", data_level="l1d", today=date(2024, 1, 1))

        self.assertEqual(
            mock_query.call_args_list,
            [
                call(instrument="mag", data_level="l1d", ingestion_start_date="20240101", ingestion_end_date="20241231", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20240101", ingestion_end_date="20240701", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20240101", ingestion_end_date="20240401", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20240101", ingestion_end_date="20240215", version="latest"),
                call(instrument="mag", data_level="l1d", ingestion_start_date="20240101", ingestion_end_date="20240123", version="latest"),
            ],
        )

    @patch(f"{MODULE}.imap_data_access.query")
    def test_propagates_non_imap_data_access_errors_without_bisection(self, mock_query):
        mock_query.side_effect = RuntimeError("transient network glitch")

        with self.assertRaises(RuntimeError):
            query_chunked_data_product(instrument="mag", data_level="l1d", today=date(2026, 5, 8))

        self.assertEqual(mock_query.call_count, 1)


if __name__ == "__main__":
    unittest.main()
