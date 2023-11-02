import ssl
import urllib
from dataclasses import dataclass
from typing import List, Dict

import certifi

from data_indexer.cdf_downloader.psp_file_parser import PspFileParser, PspFileInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.cdf_parser.variable_selector.multi_dimension_variable_selector import MultiDimensionVariableSelector
from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector

psp_isois_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/{}/l2/'
psp_fields_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/fields/l2/{}/'


@dataclass
class PspDirectoryInfo:
    base_url: str
    instrument_human_readable: str
    instrument_url: str
    file_infos_by_mode: Dict[str, List[PspFileInfo]]
    variable_selector: type[VariableSelector]

class PspDownloader:
    @staticmethod
    def get_all_metadata() -> List[PspDirectoryInfo]:
        psp_filenames = [
            PspDownloader._get_metadata_for_isois(psp_isois_cda_base_url, 'ISOIS-EPIHi', 'epihi' , DefaultVariableSelector),
                         PspDownloader._get_metadata_for_isois(psp_isois_cda_base_url, 'ISOIS-EPILo', 'epilo', MultiDimensionVariableSelector),
                         PspDownloader._get_metadata_for_isois(psp_isois_cda_base_url, 'ISOIS', 'merged', DefaultVariableSelector),
                         PspDownloader._get_metadata_for_fields(psp_fields_cda_base_url, 'FIELDS', 'mag_rtn_4_per_cycle', MultiDimensionVariableSelector),
                         ]

        return psp_filenames

    @staticmethod
    def _get_metadata_for_isois(base_url: str, instrument_human_readable: str, detector_url: str, variable_selector: type[VariableSelector]) -> PspDirectoryInfo:
        url = base_url.format(detector_url)
        file_infos_by_mode = PspFileParser.get_dictionary_of_files(url)
        return PspDirectoryInfo(base_url, instrument_human_readable, detector_url, file_infos_by_mode, variable_selector)

    @staticmethod
    def _get_metadata_for_fields(base_url: str, instrument_human_readable: str, detector_url: str, variable_selector: type[VariableSelector]) -> PspDirectoryInfo:
        url = base_url.format(detector_url)
        file_infos_by_mode = PspFileParser.get_dictionary_of_files(url, top_level_link='')
        return PspDirectoryInfo(base_url, instrument_human_readable, detector_url, file_infos_by_mode, variable_selector)

    @staticmethod
    def get_cdf_file(base_url: str, filename: str, instrument: str, category: str, year: str):
        context = ssl.create_default_context(cafile=certifi.where())

        instrument_base_url = base_url.format(instrument)
        file_url = f"{instrument_base_url}{category}{year}/{filename}"
        return {"link": file_url, "data": urllib.request.urlopen(file_url, context=context).read()}
