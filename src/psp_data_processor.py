from src.cdf_downloader.psp_downloader import PspDownloader
from src.cdf_variable_parser import CdfVariableParser


class PspDataProcessor:
    @staticmethod
    def get_metadata_index():
        index = []

        file_names_dictionary = PspDownloader.get_all_filenames()

        for instrument, category_dictionary in file_names_dictionary.items():
            for category, file_names in category_dictionary.items():
                psp_file_info = file_names[0]
                cdf = PspDownloader.get_cdf_file(psp_file_info.name, instrument, category, psp_file_info.year)
                variables = CdfVariableParser.parse_variables_from_cdf_bytes(cdf['data'])
                split_link = cdf['link'].split('_')
                rebuilt_link = '_'.join(split_link[:-2]) + '_%yyyymmdd%_' + split_link[-1]
                index.append({"descriptions": variables, "source_file_format": rebuilt_link})
        return index

if __name__ == '__main__':
    PspDataProcessor.get_metadata_index()
