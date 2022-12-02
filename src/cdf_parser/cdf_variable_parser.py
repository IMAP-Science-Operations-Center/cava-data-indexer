from dataclasses import dataclass
from typing import List

from spacepy import pycdf


@dataclass
class CdfVariableInfo:
    variable_name: str
    catalog_description: str
    display_type: str


class CdfVariableParser:

    @staticmethod
    def _check_needed_values_are_present(variable, cdf):
        has_correct_shape = (variable.attrs['DISPLAY_TYPE'] == 'spectrogram' and len(variable.shape) <= 2) or (
                variable.attrs['DISPLAY_TYPE'] == 'time_series' and len(variable.shape) == 1)
        has_field_name = 'FIELDNAM' in variable.attrs
        time_col = variable.attrs['DEPEND_0']
        has_time_delta_minus_col = 'DELTA_MINUS_VAR' in cdf[time_col].attrs
        has_time_delta_plus_col = 'DELTA_PLUS_VAR' in cdf[time_col].attrs
        zscale = variable.attrs['SCALETYP'] if 'SCALETYP' in variable.attrs else 'log'
        is_z_scale_valid = zscale == 'linear' or variable.attrs['SCALEMIN'] != 0
        time_is_ns = cdf[time_col].attrs['UNITS'] == 'ns'
        return has_correct_shape and has_field_name and has_time_delta_minus_col and has_time_delta_plus_col and is_z_scale_valid and time_is_ns

    @staticmethod
    def parse_info_from_cdf(cdf: pycdf.CDF) -> List[CdfVariableInfo]:
        variable_infos = []
        for key, var in cdf.items():
            if var.attrs["VAR_TYPE"] == "data" and CdfVariableParser._check_needed_values_are_present(var, cdf):
                catalog_description = str(var.attrs["CATDESC"])
                display_type = str(var.attrs['DISPLAY_TYPE'])
                variable_infos.append(CdfVariableInfo(key, catalog_description, display_type))
        return sorted(variable_infos, key=lambda i: i.catalog_description.lower())
