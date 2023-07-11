import urllib
from dataclasses import dataclass
from typing import List, Dict

from src.data_indexer.cdf_downloader.psp_file_parser import PspFileParser, PspFileInfo

psp_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/{}/l2/'


@dataclass
class PspDirectoryInfo:
    instrument_human_readable: str
    instrument_url: str
    file_infos_by_mode: Dict[str, List[PspFileInfo]]


class PspDownloader:
    @staticmethod
    def get_all_metadata() -> List[PspDirectoryInfo]:
        psp_filenames = [PspDownloader._get_metadata_for_instrument('ISOIS-EPIHi', 'epihi'),
                         PspDownloader._get_metadata_for_instrument('ISOIS-EPILo', 'epilo'),
                         PspDownloader._get_metadata_for_instrument('ISOIS', 'merged')]

        return psp_filenames

    @staticmethod
    def _get_metadata_for_instrument(instrument_human_readable: str, instrument_url: str) -> PspDirectoryInfo:
        url = psp_cda_base_url.format(instrument_url)
        file_infos_by_mode = PspFileParser.get_dictionary_of_files(url)
        return PspDirectoryInfo(instrument_human_readable, instrument_url, file_infos_by_mode)

    @staticmethod
    def get_cdf_file(filename: str, instrument: str, category: str, year: str):
        instrument_base_url = psp_cda_base_url.format(instrument)
        file_url = f"{instrument_base_url}{category}{year}/{filename}"
        return {"link": file_url, "data": urllib.request.urlopen(file_url).read()}
