from dataclasses import dataclass
from enum import Enum
from typing import List, Dict

from data_indexer.cdf_downloader.psp_file_parser import PspFileParser, PspFileInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.cdf_parser.variable_selector.multi_dimension_variable_selector import MultiDimensionVariableSelector
from data_indexer.cdf_parser.variable_selector.omni_variable_selector import OmniVariableSelector
from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector
from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence
from data_indexer.file_cadence.file_cadence import FileCadence
from data_indexer.file_cadence.six_month_file_cadence import SixMonthFileCadence
from data_indexer.http_client import http_client, get_with_retry

psp_isois_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/{}/l2/'
psp_fields_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/fields/l2/{}/'
omni_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/omni/omni_cdaweb/{}/'


@dataclass
class PspDirectoryInfo:
    base_url: str
    instrument_human_readable: str
    instrument_url: str
    file_infos_by_mode: Dict[str, List[PspFileInfo]]
    variable_selector: type[VariableSelector]
    mission: str
    file_cadence: FileCadence

    def __eq__(self, other):
        return self.base_url == other.base_url and \
            self.instrument_human_readable == other.instrument_human_readable and \
            self.instrument_url == other.instrument_url and \
            self.file_infos_by_mode == other.file_infos_by_mode and \
            self.variable_selector == other.variable_selector and \
            self.mission == other.mission and \
            self.file_cadence.name == other.file_cadence.name


class PspDownloader:
    @staticmethod
    def get_all_metadata() -> List[PspDirectoryInfo]:
        psp_filenames = [
            PspDownloader._get_metadata_for_multiple_data_sets(psp_isois_cda_base_url, 'ISOIS-EPIHi', 'epihi',
                                                               DefaultVariableSelector, 'PSP', DailyFileCadence()),
            PspDownloader._get_metadata_for_multiple_data_sets(psp_isois_cda_base_url, 'ISOIS-EPILo', 'epilo',
                                                               MultiDimensionVariableSelector, 'PSP',
                                                               DailyFileCadence()),
            PspDownloader._get_metadata_for_multiple_data_sets(psp_isois_cda_base_url, 'ISOIS', 'merged',
                                                               DefaultVariableSelector, 'PSP', DailyFileCadence()),
            PspDownloader._get_metadata_for_one_data_set(psp_fields_cda_base_url, 'FIELDS', 'mag_rtn_4_per_cycle',
                                                         MultiDimensionVariableSelector, 'PSP', DailyFileCadence()),
            PspDownloader._get_metadata_for_one_data_set(psp_fields_cda_base_url, 'FIELDS', 'mag_rtn_1min',
                                                         MultiDimensionVariableSelector, 'PSP', DailyFileCadence()),
            PspDownloader._get_metadata_for_one_data_set(omni_cda_base_url, 'OMNI', 'hourly', OmniVariableSelector,
                                                         'OMNI', SixMonthFileCadence())
        ]

        return psp_filenames

    @staticmethod
    def _get_metadata_for_multiple_data_sets(base_url: str, instrument_human_readable: str, detector_url: str,
                                             variable_selector: type[VariableSelector], mission: str,
                                             file_cadence: FileCadence) -> PspDirectoryInfo:
        url = base_url.format(detector_url)
        file_infos_by_mode = PspFileParser.get_dictionary_of_files(url)
        return PspDirectoryInfo(base_url, instrument_human_readable, detector_url, file_infos_by_mode,
                                variable_selector, mission, file_cadence)

    @staticmethod
    def _get_metadata_for_one_data_set(base_url: str, instrument_human_readable: str, detector_url: str,
                                       variable_selector: type[VariableSelector], mission: str,
                                       file_cadence: FileCadence) -> PspDirectoryInfo:
        url = base_url.format(detector_url)
        file_infos_by_mode = PspFileParser.get_dictionary_of_files(url, top_level_link='')
        return PspDirectoryInfo(base_url, instrument_human_readable, detector_url, file_infos_by_mode,
                                variable_selector, mission, file_cadence)

    @staticmethod
    def get_url(base_url: str, filename: str, instrument: str, category: str, year: str):
        instrument_base_url = base_url.format(instrument)
        return f"{instrument_base_url}{category}{year}/{filename}"

    @staticmethod
    def get_cdf_file(base_url: str, filename: str, instrument: str, category: str, year: str):
        instrument_base_url = base_url.format(instrument)
        file_url = f"{instrument_base_url}{category}{year}/{filename}"
        return {"link": file_url, "data": get_with_retry(file_url).content}
