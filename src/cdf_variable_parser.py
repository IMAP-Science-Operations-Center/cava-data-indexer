import os
import tempfile
from typing import Dict

from spacepy import pycdf


class CdfVariableParser:

    @staticmethod
    def parse_variables_from_cdf(file_path: str) -> Dict[str, str]:
        cdf = pycdf.CDF(file_path)
        version = cdf.attrs['Data_version']
        return {f'{var.attrs["CATDESC"]} v{version}': key for key, var in cdf.items()
                if var.attrs["VAR_TYPE"] == "data"}

    @staticmethod
    def parse_variables_from_cdf_bytes(cdf_bytes: bytes):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, 'cdf.cdf'), 'wb') as tmp_file:
                tmp_file.write(cdf_bytes)
            return CdfVariableParser.parse_variables_from_cdf(tmp_file.name)