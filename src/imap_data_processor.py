from collections import defaultdict
from datetime import datetime

from src import dates_available
from src.cdf_downloader.imap_downloader import get_all_metadata, get_cdf_file
from src.cdf_variable_parser import CdfVariableParser


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
        cdf = get_cdf_file(matching_files_metadata[0]["file_name"])
        variables = CdfVariableParser.parse_info_from_cdf_bytes(cdf["data"])
        link = cdf["link"].replace(matching_files_metadata[0]["file_name"], file_name_format)
        index.append({"descriptions": variables.variable_desc_to_key_dict, "source_file_format": link, "description_source_file": cdf["link"],
                      "dates_available": [[str(date_range[0]), str(date_range[1])] for date_range in available_dates],
                      "logical_source": variables.logical_source,
                      "version": variables.data_version})
    return index


if __name__ == '__main__':
    get_metadata_index()
