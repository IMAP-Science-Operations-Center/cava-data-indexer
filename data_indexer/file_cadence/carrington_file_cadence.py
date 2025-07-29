from datetime import datetime, timedelta, timezone

from data_indexer.file_cadence.file_cadence import FileCadence

CARRINGTON_ROTATION_IN_DAYS = timedelta(days=27.2753)
FIRST_CARRINGTON_ROTATION = datetime(1853, 10, 13, 13, 17, 19, 679992, tzinfo=timezone.utc)


class CarringtonFileCadence(FileCadence):
    @property
    def name(self):
        return 'carrington_rotation'

    def get_file_time_range_with_cr(self, cr: int) -> tuple[datetime, datetime]:
        start_time = cr * CARRINGTON_ROTATION_IN_DAYS + FIRST_CARRINGTON_ROTATION
        end_time = (cr + 1) * CARRINGTON_ROTATION_IN_DAYS + FIRST_CARRINGTON_ROTATION

        return start_time, end_time

    def get_file_time_range(self, t: datetime) -> tuple[datetime, datetime]:
        t_within_carrington = t + timedelta(days=1)
        cr = (t_within_carrington - FIRST_CARRINGTON_ROTATION) // CARRINGTON_ROTATION_IN_DAYS
        return self.get_file_time_range_with_cr(cr)
