from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from spacepy import pycdf


@dataclass
class CdfGlobalInfo:
    logical_source: str
    logical_source_description: str
    data_version: str
    generation_date: date


class CdfGlobalParser:
    @staticmethod
    def parse_global_variables_from_cdf(cdf: pycdf.CDF) -> CdfGlobalInfo:
        logical_source = str(cdf.attrs['Logical_source'])
        logical_source_description = str(cdf.attrs['Logical_source_description'])
        data_version = str(cdf.attrs['Data_version'])
        generation_date_string = str(cdf.attrs['Generation_date'])
        time = parse_time(generation_date_string)
        if time is None:
            filename = Path(cdf.pathname.decode()).name
            raise ValueError(f"Failed to parse generation date `{generation_date_string}` from CDF `{filename}`")
        generation_date = time.date()
        return CdfGlobalInfo(logical_source, logical_source_description, data_version, generation_date)

def parse_time(time_string: str) -> datetime:
    formats = ['%Y%m%d', '%a %b %d %H:%M:%S %Y']
    for format in formats:
        try:
            return datetime.strptime(time_string, format)
        except ValueError:
            pass

