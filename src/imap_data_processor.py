import os
import tempfile
from collections import defaultdict
from datetime import datetime

from src.cdf_downloader.imap_downloader import get_all_metadata, get_cdf_file
from src.cdf_global_parser import CdfGlobalParser
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
        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, 'cdf.cdf'), 'wb') as tmp_file:
                tmp_file.write(cdf["data"])
                variables = CdfVariableParser.parse_variables_from_cdf(tmp_file.name)
                global_variables = CdfGlobalParser.parse_global_variables_from_cdf(tmp_file.name)
        link = cdf["link"].replace(filenames[0]["file_name"], file_name_format)
        index.append({"descriptions": variables, "source_file_format": link, "description_source_file": cdf["link"], "logical_source": global_variables['Logical_source']})
    return index


if __name__ == '__main__':
    get_metadata_index()
