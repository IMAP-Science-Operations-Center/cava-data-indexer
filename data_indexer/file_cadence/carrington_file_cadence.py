from datetime import datetime, timedelta, timezone

from data_indexer.file_cadence.file_cadence import FileCadence

CARRINGTON_ROTATION_IN_DAYS = timedelta(days=27.2753)
FIRST_CARRINGTON_ROTATION = datetime(1853, 10, 13, tzinfo=timezone.utc) + timedelta(days=0.6016)


class CarringtonFileCadence(FileCadence):
    name = 'carrington_rotation'

    @staticmethod
    def get_file_time_range_with_cr(cr: int) -> tuple[datetime, datetime]:
        start_time = cr * CARRINGTON_ROTATION_IN_DAYS + FIRST_CARRINGTON_ROTATION
        end_time = (cr + 1) * CARRINGTON_ROTATION_IN_DAYS + FIRST_CARRINGTON_ROTATION

        return start_time, end_time

    @staticmethod
    def get_file_time_range(t: datetime) -> tuple[datetime, datetime]:
        t_within_carrington = t + timedelta(days=1)
        cr = (t_within_carrington - FIRST_CARRINGTON_ROTATION) // CARRINGTON_ROTATION_IN_DAYS
        return CarringtonFileCadence.get_file_time_range_with_cr(cr)
