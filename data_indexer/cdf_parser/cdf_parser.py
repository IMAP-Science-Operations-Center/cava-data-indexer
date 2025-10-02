import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List

from spacepy import pycdf

from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalInfo, CdfGlobalParser
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableParser, CdfVariableInfo
from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector


@dataclass
class CdfFileInfo:
    global_info: CdfGlobalInfo
    variable_infos: List[CdfVariableInfo]


class CdfParser:

    @staticmethod
    def parse_cdf_bytes(cdf_bytes: bytes, variable_selector: type[VariableSelector]) -> CdfFileInfo:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, 'temp.cdf'), 'wb') as tmp_file:
                tmp_file.write(cdf_bytes)
            return CdfParser.parse_cdf(tmp_file.name, variable_selector)

    @staticmethod
    def parse_cdf(cdf_path: Path, variable_selector: type[VariableSelector]):
        with pycdf.CDF(str(cdf_path)) as cdf:
            cdf_global_info = CdfGlobalParser.parse_global_variables_from_cdf(cdf)
            cdf_variable_info = CdfVariableParser.parse_info_from_cdf(cdf, variable_selector)
            return CdfFileInfo(cdf_global_info, cdf_variable_info)
