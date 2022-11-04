import os.path
import tempfile
from collections import defaultdict
from datetime import datetime
from typing import List
from spacepy import pycdf

from src.data_downloader import get_all_metadata, get_cdf_file


def group_metadata_by_file_names(metadata: [{}]) -> [{}]:
    grouped_files = defaultdict(list)

    for file in metadata:
        timetag_date = datetime.fromisoformat(file['timetag'])
        timetag_date_string = timetag_date.strftime('%Y%m%d')
        if timetag_date_string in file["file_name"]:
            filename_template = file["file_name"].replace(timetag_date_string, "%yyyymmdd%")
            grouped_files[filename_template].append(file)

    return grouped_files


def _parse_variables_from_cdf(file_path: str) -> List[str]:
    cdf = pycdf.CDF(file_path)
    return [var.attrs["CATDESC"] for var in cdf.values()
            if var.attrs["VAR_TYPE"] == "data"]

def parse_variables_from_cdf_bytes(cdf_bytes: bytes):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with open(os.path.join(tmp_dir, 'cdf.cdf'), 'wb') as tmp_file:
            tmp_file.write(cdf_bytes)
        return _parse_variables_from_cdf(tmp_file.name)

def get_metadata_index():
    index = []
    md = group_metadata_by_file_names(get_all_metadata())
    for file_name_format, filenames in sorted(md.items()):
        cdf = get_cdf_file(filenames[0]["file_name"])
        variables = parse_variables_from_cdf_bytes(cdf)
        index.append({"descriptions": variables, "source_file_format": file_name_format})
    return index

if __name__ == '__main__':
    get_metadata_index()
