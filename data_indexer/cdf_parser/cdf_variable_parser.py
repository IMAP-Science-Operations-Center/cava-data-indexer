from dataclasses import dataclass
from typing import List, Optional

from spacepy import pycdf

from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector


@dataclass
class CdfVariableInfo:
    variable_name: str
    catalog_description: str
    display_type: str
    units: Optional[str]
    axis_label: Optional[str]


class CdfVariableParser:

    @staticmethod
    def parse_info_from_cdf(cdf: pycdf.CDF, selector: type[VariableSelector]) -> List[CdfVariableInfo]:
        variable_infos = []
        for key, var in cdf.items():
            if selector.should_include(var, cdf):
                catalog_description = str(var.attrs["CATDESC"])
                display_type = str(var.attrs['DISPLAY_TYPE'])
                units = str(var.attrs.get("UNITS"))
                axis_label = str(var.attrs.get("LABLAXIS", ""))
                variable_infos.append(CdfVariableInfo(key, catalog_description, display_type, units, axis_label))
            elif var.attrs["VAR_TYPE"] == "data":
                print("Ignored variable", key, "from file", cdf.attrs["Logical_source"])

        return sorted(variable_infos, key=lambda i: i.catalog_description.lower())
