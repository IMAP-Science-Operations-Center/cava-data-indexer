from typing import Dict

from spacepy import pycdf


class CdfGlobalParser:
    @staticmethod
    def parse_global_variables_from_cdf(file_path: str) -> Dict[str, str]:
        cdf = pycdf.CDF(file_path)
        logical_source = cdf.attrs['Logical_source']
        return {"Logical_source": str(logical_source)}
