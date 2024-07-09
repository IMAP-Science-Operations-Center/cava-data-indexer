from datetime import datetime, timedelta

from data_indexer.file_cadence.file_cadence import FileCadence


class DailyFileCadence(FileCadence):
    name = 'daily'
    @staticmethod
    def get_file_time_range(t: datetime) -> tuple[datetime,datetime]:
        start_time = t.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)
        return start_time, end_time
