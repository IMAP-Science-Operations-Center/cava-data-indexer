from datetime import date, timedelta,datetime
from typing import List

from data_indexer.cdf_downloader.psp_file_parser import PspFileInfo
from data_indexer.file_cadence.file_cadence import FileCadence


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


def get_contiguous_ranges(ranges: list[tuple[datetime,datetime]]) -> list[tuple[datetime,datetime]]:
    sorted_ranges = sorted(ranges)
    contiguous_ranges = []
    contiguous_start, contiguous_end = sorted_ranges[0]
    for range_start,range_end in sorted_ranges[1:]:
        if range_start > contiguous_end:
            contiguous_ranges.append((contiguous_start,contiguous_end))
            contiguous_start = range_start
        contiguous_end = range_end
    contiguous_ranges.append((contiguous_start,contiguous_end))
    return contiguous_ranges

def get_date_ranges_from_file_infos(file_infos: list[PspFileInfo], cadence: type[FileCadence]) -> list[list[date]]:
    parsed_dates = [datetime.strptime(file_info.name.split('_')[-2], '%Y%m%d') for file_info in
                    file_infos]
    date_ranges = []
    for date in parsed_dates:
        date_ranges.append(cadence.get_file_time_range(date))

    return [[start.date(), end.date()-timedelta(days=1)] for start, end in get_contiguous_ranges(date_ranges)]
