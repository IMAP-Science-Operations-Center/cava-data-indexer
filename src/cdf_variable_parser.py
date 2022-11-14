import os
import tempfile
from typing import Dict

from spacepy import pycdf


class CdfVariableParser:

    @staticmethod
    def _check_needed_values_are_present(variable, cdf):
        has_correct_shape = len(variable.shape) <= 2
        has_field_name = 'FIELDNAM' in variable.attrs
        time_col = variable.attrs['DEPEND_0']
        has_time_delta_minus_col = 'DELTA_MINUS_VAR' in cdf[time_col].attrs
        has_time_delta_plus_col = 'DELTA_PLUS_VAR' in cdf[time_col].attrs
        zscale = variable.attrs['SCALETYP'] if 'SCALETYP' in variable.attrs else 'log'
        is_z_scale_valid = zscale == 'linear' or variable.attrs['SCALEMIN'] != 0
        # time_is_ns = cdf[time_col].attrs['UNITS'] == 'ns'
        return has_correct_shape and has_field_name and has_time_delta_minus_col and has_time_delta_plus_col and is_z_scale_valid # and time_is_ns

    @staticmethod
    def parse_variables_from_cdf(file_path: str) -> Dict[str, str]:
        cdf = pycdf.CDF(file_path)
        version = cdf.attrs['Data_version']
        return {f'{var.attrs["CATDESC"]} v{version}': key for key, var in cdf.items()
                if var.attrs["VAR_TYPE"] == "data" and CdfVariableParser._check_needed_values_are_present(var, cdf)}

    @staticmethod
    def parse_variables_from_cdf_bytes(cdf_bytes: bytes):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, 'cdf.cdf'), 'wb') as tmp_file:
                tmp_file.write(cdf_bytes)
            return CdfVariableParser.parse_variables_from_cdf(tmp_file.name)
