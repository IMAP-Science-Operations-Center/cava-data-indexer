from dataclasses import dataclass
from typing import Dict

from spacepy import pycdf


@dataclass
class CdfGlobalInfo:
    logical_source: str
    logical_source_description: str
    data_version: str


class CdfGlobalParser:
    @staticmethod
    def parse_global_variables_from_cdf(cdf : pycdf.CDF) -> CdfGlobalInfo:
        logical_source = str(cdf.attrs['Logical_source'])
        logical_source_description = str(cdf.attrs['Logical_source_description'])
        data_version = str(cdf.attrs['Data_version'])
        return CdfGlobalInfo(logical_source,logical_source_description,  data_version)
