from datetime import datetime

from data_indexer import utils, dates_available
from data_indexer.cdf_downloader.psp_downloader import PspDownloader, FileCadence
from data_indexer.cdf_parser.cdf_parser import CdfParser


class PspDataProcessor:

    @staticmethod
    def get_metadata_index():
        index = []

        psp_directory_infos = PspDownloader.get_all_metadata()

        for psp_directory_info in psp_directory_infos:
            for category, file_infos in psp_directory_info.file_infos_by_mode.items():

                available_dates = dates_available.get_date_ranges_from_file_infos(file_infos,psp_directory_info.file_cadence)
                psp_file_info = file_infos[0]
                cdf = PspDownloader.get_cdf_file(psp_directory_info.base_url, psp_file_info.name, psp_directory_info.instrument_url, category,
                                                 psp_file_info.year)
                cdf_info = CdfParser.parse_cdf_bytes(cdf["data"], psp_directory_info.variable_selector)
                split_link = cdf['link'].split('_')
                rebuilt_link = '_'.join(split_link[:-2]) + '_%yyyymmdd%_' + split_link[-1]
                rebuilt_link = rebuilt_link.replace(psp_file_info.year, '%yyyy%')
                index.append(utils.get_index_entry(cdf_info, rebuilt_link, cdf['link'], available_dates,
                                                   psp_directory_info.instrument_human_readable, psp_directory_info.mission,psp_directory_info.file_cadence))
        return index


if __name__ == '__main__':
    PspDataProcessor.get_metadata_index()
