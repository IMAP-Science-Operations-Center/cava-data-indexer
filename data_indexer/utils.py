import dataclasses
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List

from data_indexer.cdf_parser.cdf_parser import CdfFileInfo
from data_indexer.file_cadence.file_cadence import FileCadence


@dataclass
class DataProductSource:
    url: str
    start_time: datetime
    end_time: datetime


def get_index_entry(cdf_file_info: CdfFileInfo, file_timeranges: list[DataProductSource], instrument: str, mission: str,
                    file_cadence: FileCadence, version: str = "") -> Dict:

    return {"variables": [dataclasses.asdict(info) for info in cdf_file_info.variable_infos],
            "logical_source": cdf_file_info.global_info.logical_source,
            "logical_source_description": cdf_file_info.global_info.logical_source_description,
            "generation_date": str(cdf_file_info.global_info.generation_date),
            "instrument": instrument,
            "mission": mission,
            "file_cadence": file_cadence.name,
            "version": version,
            "file_timeranges": [
                {
                    "start_time": file_timerange.start_time.isoformat(),
                    "end_time": file_timerange.end_time.isoformat(),
                    "url": file_timerange.url,
                } for file_timerange in file_timeranges]
            }
