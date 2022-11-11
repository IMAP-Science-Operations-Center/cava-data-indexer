import urllib

from src.cdf_downloader.psp_file_parser import PspFileParser

psp_cda_base_url = 'https://cdaweb.gsfc.nasa.gov/pub/data/psp/isois/{}/l2/'


class PspDownloader:
    @staticmethod
    def get_all_filenames():
        psp_filenames = {}

        PspDownloader._get_filenames_for_instrument('epihi', psp_filenames)
        PspDownloader._get_filenames_for_instrument('epilo', psp_filenames)
        PspDownloader._get_filenames_for_instrument('merged', psp_filenames)

        return psp_filenames

    @staticmethod
    def _get_filenames_for_instrument(instrument: str, psp_filenames: {}) -> None:
        url = psp_cda_base_url.format(instrument)
        filenames = PspFileParser.get_dictionary_of_files(url)
        psp_filenames[instrument] = filenames

    @staticmethod
    def get_cdf_file(filename: str, instrument: str, category: str, year: str):
        instrument_base_url = psp_cda_base_url.format(instrument)
        file_url = f"{instrument_base_url}{category}/{year}/{filename}"
        return {"link": file_url, "data": urllib.request.urlopen(file_url).read()}
