from dataclasses import dataclass
from enum import Enum
from typing import List, Dict

from data_indexer.cdf_downloader.psp_file_parser import PspFileParser, PspFileInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.cdf_parser.variable_selector.multi_dimension_variable_selector import MultiDimensionVariableSelector
from data_indexer.cdf_parser.variable_selector.omni_variable_selector import OmniVariableSelector
from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector
from data_indexer.http_client import http_client

psp_isois_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/{}/l2/'
psp_fields_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/fields/l2/{}/'
omni_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/omni/omni_cdaweb/{}/'

class FileCadence(Enum):
    DAILY = 'daily'
    SIX_MONTH = 'six_month'

@dataclass
class PspDirectoryInfo:
    base_url: str
    instrument_human_readable: str
    instrument_url: str
    file_infos_by_mode: Dict[str, List[PspFileInfo]]
    variable_selector: type[VariableSelector]
    mission: str
    file_cadence: FileCadence


class PspDownloader:
    @staticmethod
    def get_all_metadata() -> List[PspDirectoryInfo]:
        psp_filenames = [
            PspDownloader._get_metadata_for_multiple_data_sets(psp_isois_cda_base_url, 'ISOIS-EPIHi', 'epihi',
                                                  DefaultVariableSelector,'PSP',FileCadence.DAILY),
            PspDownloader._get_metadata_for_multiple_data_sets(psp_isois_cda_base_url, 'ISOIS-EPILo', 'epilo',
                                                  MultiDimensionVariableSelector,'PSP',FileCadence.DAILY),
            PspDownloader._get_metadata_for_multiple_data_sets(psp_isois_cda_base_url, 'ISOIS', 'merged', DefaultVariableSelector,'PSP',FileCadence.DAILY),
            PspDownloader._get_metadata_for_one_data_set(psp_fields_cda_base_url, 'FIELDS', 'mag_rtn_4_per_cycle',
                                                   MultiDimensionVariableSelector,'PSP',FileCadence.DAILY),
            PspDownloader._get_metadata_for_one_data_set(psp_fields_cda_base_url, 'FIELDS', 'mag_rtn_1min',
                                                   MultiDimensionVariableSelector,'PSP',FileCadence.DAILY),
            PspDownloader._get_metadata_for_one_data_set(omni_cda_base_url,'OMNI','hourly',OmniVariableSelector,'OMNI',FileCadence.SIX_MONTH)
        ]

        return psp_filenames

    @staticmethod
    def _get_metadata_for_multiple_data_sets(base_url: str, instrument_human_readable: str, detector_url: str,
                                variable_selector: type[VariableSelector], mission: str,file_cadence:FileCadence) -> PspDirectoryInfo:
        url = base_url.format(detector_url)
        file_infos_by_mode = PspFileParser.get_dictionary_of_files(url)
        return PspDirectoryInfo(base_url, instrument_human_readable, detector_url, file_infos_by_mode,
                                variable_selector,mission,file_cadence)

    @staticmethod
    def _get_metadata_for_one_data_set(base_url: str, instrument_human_readable: str, detector_url: str,
                                 variable_selector: type[VariableSelector], mission: str,file_cadence:FileCadence) -> PspDirectoryInfo:
        url = base_url.format(detector_url)
        file_infos_by_mode = PspFileParser.get_dictionary_of_files(url, top_level_link='')
        return PspDirectoryInfo(base_url, instrument_human_readable, detector_url, file_infos_by_mode,
                                variable_selector,mission,file_cadence)

    @staticmethod
    def get_cdf_file(base_url: str, filename: str, instrument: str, category: str, year: str):
        instrument_base_url = base_url.format(instrument)
        file_url = f"{instrument_base_url}{category}{year}/{filename}"
        return {"link": file_url, "data": http_client.get(file_url).content}
