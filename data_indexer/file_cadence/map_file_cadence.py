from datetime import timedelta, datetime

from data_indexer.file_cadence.file_cadence import FileCadence


class BadFileNameException(Exception):
    pass


class MapFileCadence(FileCadence):
    def __init__(self, duration: str):
        one_year = timedelta(days=365.25)

        duration_to_name_and_timedelta = {
            "1mo": ("one_month_map", one_year / 12),
            "3mo": ("three_month_map", one_year / 4),
            "6mo": ("six_month_map", one_year / 2),
            "1yr": ("one_year_map", one_year),
        }
        if duration not in duration_to_name_and_timedelta:
            raise BadFileNameException(f"Cannot parse map with cadence: {duration}")

        self._name, self._duration = duration_to_name_and_timedelta.get(duration)

    @property
    def name(self):
        return self._name

    def get_file_time_range(self, t: datetime) -> tuple[datetime, datetime]:
        return t, t + self._duration
