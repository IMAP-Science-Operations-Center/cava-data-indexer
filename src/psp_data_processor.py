from datetime import datetime

from src import dates_available, utils
from src.cdf_downloader.psp_downloader import PspDownloader
from src.cdf_parser.cdf_parser import CdfParser


class PspDataProcessor:
    @staticmethod
    def get_metadata_index():
        index = []

        file_names_dictionary = PspDownloader.get_all_filenames()

        for instrument, category_dictionary in file_names_dictionary.items():
            for category, file_infos in category_dictionary.items():
                parsed_dates = [datetime.strptime(file_info.name.split('_')[-2], '%Y%m%d').date() for file_info in
                                file_infos]
                available_dates = dates_available.get_date_ranges(parsed_dates)
                psp_file_info = file_infos[0]
                cdf = PspDownloader.get_cdf_file(psp_file_info.name, instrument, category, psp_file_info.year)
                cdf_info = CdfParser.parse_cdf_bytes(cdf["data"])
                split_link = cdf['link'].split('_')
                rebuilt_link = '_'.join(split_link[:-2]) + '_%yyyymmdd%_' + split_link[-1]
                rebuilt_link = rebuilt_link.replace(psp_file_info.year, '%yyyy%')
                index.append(utils.get_index_entry(cdf_info, rebuilt_link, cdf['link'], available_dates))
        return index


if __name__ == '__main__':
    PspDataProcessor.get_metadata_index()
