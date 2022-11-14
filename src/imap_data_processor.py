from collections import defaultdict
from datetime import datetime

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
    for file_name_format, filenames in sorted(metadata.items()):
        cdf = get_cdf_file(filenames[0]["file_name"])
        variables = CdfVariableParser.parse_variables_from_cdf_bytes(cdf["data"])
        link = cdf["link"].replace(filenames[0]["file_name"], file_name_format)
        index.append({"descriptions": variables, "source_file_format": link})
    return index


if __name__ == '__main__':
    get_metadata_index()
