import time
from datetime import date, timedelta
from pathlib import Path

import imap_data_access
from imap_data_access.io import IMAPDataAccessError

INDEX_START_YEAR: int = 2024
MIN_BISECT_RANGE: timedelta = timedelta(days=30)


def get_with_retry(description_source_file, times: int = 5) -> Path:
    for i in range(times):
        try:
            return imap_data_access.download(description_source_file)
        except Exception as e:
            if i == times - 1:
                raise e
            print(f"Retrying get for url {description_source_file}; retry number {i + 1}; exception {e}")
            time.sleep(2**i)


def query_chunked_data_product(instrument: str, data_level: str, today: date) -> list[dict]:
    results: list[dict] = []
    for year in range(INDEX_START_YEAR, today.year + 1):
        results.extend(_query_range(instrument, data_level, date(year, 1, 1), date(year, 12, 31)))
    return results


def _query_range(instrument: str, data_level: str, start: date, end: date) -> list[dict]:
    try:
        return imap_data_access.query(
            instrument=instrument,
            data_level=data_level,
            ingestion_start_date=start.strftime("%Y%m%d"),
            ingestion_end_date=end.strftime("%Y%m%d"),
        )
    except IMAPDataAccessError:
        if end - start < MIN_BISECT_RANGE:
            raise
        return _recursively_query_bisected_halves(instrument, data_level, start, end)


def _recursively_query_bisected_halves(
    instrument: str, data_level: str, start: date, end: date
) -> list[dict]:
    midpoint = start + (end - start) // 2
    return _query_range(instrument, data_level, start, midpoint) + _query_range(
        instrument, data_level, midpoint + timedelta(days=1), end
    )
