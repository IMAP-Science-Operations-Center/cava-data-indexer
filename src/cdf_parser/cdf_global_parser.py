from dataclasses import dataclass
from typing import Dict

from spacepy import pycdf


@dataclass
class CdfGlobalInfo:
    logical_source: str
    data_version: str


class CdfGlobalParser:
    @staticmethod
    def parse_global_variables_from_cdf(cdf : pycdf.CDF) -> CdfGlobalInfo:
        logical_source = str(cdf.attrs['Logical_source'])
        data_version = str(cdf.attrs['Data_version'])
        return CdfGlobalInfo(logical_source, data_version)
