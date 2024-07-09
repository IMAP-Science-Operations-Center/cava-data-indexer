from abc import ABC, abstractmethod
from datetime import datetime



class FileCadence(ABC):
    name = ''
    @staticmethod
    @abstractmethod
    def get_file_time_range(t: datetime) -> tuple[datetime,datetime]:
        pass



