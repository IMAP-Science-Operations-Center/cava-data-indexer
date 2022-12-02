from dataclasses import dataclass
from datetime import date, datetime

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
        generation_date = datetime.strptime(str(cdf.attrs['Generation_date']), '%Y%m%d').date()
        return CdfGlobalInfo(logical_source, logical_source_description, data_version, generation_date)
