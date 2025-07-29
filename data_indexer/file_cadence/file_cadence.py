from abc import ABC, abstractmethod
from datetime import datetime


class FileCadence(ABC):

    @property
    @abstractmethod
    def name(self):
        raise NotImplementedError

    @abstractmethod
    def get_file_time_range(self, t: datetime) -> tuple[datetime,datetime]:
        pass
