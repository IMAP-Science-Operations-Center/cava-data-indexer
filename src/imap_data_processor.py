from collections import defaultdict
from datetime import datetime

from src import dates_available, utils
from src.cdf_downloader.imap_downloader import get_all_metadata, get_cdf_file
from src.cdf_parser.cdf_parser import CdfParser


def group_metadata_by_file_names(metadata: [{}]) -> [{}]:
    grouped_files = defaultdict(list)

    for file in metadata:
        timetag_date = datetime.fromisoformat(file['timetag'])
        timetag_date_string = timetag_date.strftime('%Y%m%d')
        if timetag_date_string in file["file_name"]:
            filename_template = file["file_name"].replace(timetag_date_string, "%yyyymmdd%")
            grouped_files[filename_template].append(file)

    return grouped_files


def get_metadata_index():
    index = []
    metadata = group_metadata_by_file_names(get_all_metadata())
    for file_name_format, matching_files_metadata in sorted(metadata.items()):
        sorted_files_metadata = sorted(matching_files_metadata, key=lambda m: m["timetag"])
        sorted_dates = [datetime.fromisoformat(metadata["timetag"]).date() for metadata in sorted_files_metadata]
        available_dates = dates_available.get_date_ranges(sorted_dates)
        first_file_metadata = matching_files_metadata[0]
        cdf = get_cdf_file(first_file_metadata["file_name"])
        cdf_file_info = CdfParser.parse_cdf_bytes(cdf["data"])
        link = cdf["link"].replace(first_file_metadata["file_name"], file_name_format)
        instrument = _capitalize_isois_instrument_name(first_file_metadata["instrument_id"])
        index.append(utils.get_index_entry(cdf_file_info, link, cdf["link"],
                                           available_dates, instrument, "IMAP"))
    return index

def _capitalize_isois_instrument_name(instrument_name: str):
    return 'ISOIS' if instrument_name == 'isois' else instrument_name

if __name__ == '__main__':
    get_metadata_index()
