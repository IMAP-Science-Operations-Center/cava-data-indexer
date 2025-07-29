from datetime import datetime

from data_indexer.file_cadence.file_cadence import FileCadence


class SixMonthFileCadence(FileCadence):
    @property
    def name(self):
        return 'six_month'

    def get_file_time_range(self, t: datetime) -> tuple[datetime,datetime]:
        if t.month < 7:
            start_time = t.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time.replace(month=7)
        else:
            start_time = t.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time.replace(year=start_time.year + 1, month=1)
        return start_time, end_time
