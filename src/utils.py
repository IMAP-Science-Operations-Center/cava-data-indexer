from datetime import date
from typing import Dict, List

from src.cdf_parser.cdf_parser import CdfFileInfo


def get_index_entry(cdf_file_info: CdfFileInfo, source_file_format: str, description_source_file: str,
                    available_dates: List[List[date]]) -> Dict:
    return {"descriptions": cdf_file_info.variable_desc_to_key_dict, "source_file_format": source_file_format,
            "description_source_file": description_source_file,
            "dates_available": [[str(date_range[0]), str(date_range[1])] for date_range in available_dates],
            "logical_source": cdf_file_info.global_info.logical_source,
            "logical_source_description": cdf_file_info.global_info.logical_source_description,
            "version": cdf_file_info.global_info.data_version}
