from datetime import date, timedelta
from typing import List


def get_date_ranges(sorted_dates: List[date]) -> List[List[date]]:
    date_ranges = []
    range_start = sorted_dates[0]
    previous_file_date = sorted_dates[0]
    for file_date in sorted_dates[1:]:
        if file_date - previous_file_date > timedelta(days=1):
            date_ranges.append([range_start, previous_file_date])
            range_start = file_date
        previous_file_date = file_date
    date_ranges.append([range_start, previous_file_date])
    return date_ranges
