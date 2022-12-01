import os
import tempfile

from src.cdf_downloader.psp_downloader import PspDownloader
from src.cdf_global_parser import CdfGlobalParser
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
                with tempfile.TemporaryDirectory() as tmp_dir:
                    with open(os.path.join(tmp_dir, 'cdf.cdf'), 'wb') as tmp_file:
                        tmp_file.write(cdf["data"])
                        variables = CdfVariableParser.parse_variables_from_cdf(tmp_file.name)
                        global_variables = CdfGlobalParser.parse_global_variables_from_cdf(tmp_file.name)
                split_link = cdf['link'].split('_')
                rebuilt_link = '_'.join(split_link[:-2]) + '_%yyyymmdd%_' + split_link[-1]
                rebuilt_link = rebuilt_link.replace(psp_file_info.year, '%yyyy%')
                index.append({"descriptions": variables, "source_file_format": rebuilt_link,
                              "description_source_file": cdf['link'],
                              "logical_source": global_variables['Logical_source']})
        return index


if __name__ == '__main__':
    PspDataProcessor.get_metadata_index()
