import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import TypeVar

import imap_data_access

from data_indexer.cdf_downloader.imap_downloader import get_cdf_file_from_sdc
from data_indexer.cdf_parser.cdf_parser import CdfParser
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence
from data_indexer.utils import get_index_entry


@dataclass(frozen=True)
class VariableIndex:
    variable_name: str
    catalog_description: str
    display_type: str

@dataclass(frozen=True)
class Dataproduct:
    instrument: str
    data_level: str
    descriptor: str

@dataclass(frozen=True)
class DataproductMetadata:
    variables: list[VariableIndex]
    source_file_format: str
    description_source_file: str
    logical_source: str
    logical_source_description: str
    version: str
    generation_date: str
    instrument: str
    mission: str
    file_cadence: str

def group_metadata_by_file_names(metadata: [{}]) -> [{}]:
    grouped_files = defaultdict(list)

    for file in metadata:
        timetag_date = datetime.fromisoformat(file['timetag'])
        timetag_date_string = timetag_date.strftime('%Y%m%d')
        if timetag_date_string in file["file_name"]:
            filename_template = file["file_name"].replace(timetag_date_string, "%yyyymmdd%")
            grouped_files[filename_template].append(file)

    return grouped_files

T = TypeVar('T')

def flatten(l: list[list[T]]) -> list[T]:
    return reduce(list.__add__, l, [])

def get_metadata_index() -> list[dict]:
    uuid_matcher = re.compile("-fake-menlo-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}|-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")

    l3_version_variants = ["l3", "l3a", "l3b"]
    l3_cdf_metadatas = flatten([imap_data_access.query(data_level=version) for version in l3_version_variants])

    data_products = defaultdict(lambda: defaultdict(list))
    for cdf_metadata in l3_cdf_metadatas:
        descriptor = cdf_metadata["descriptor"]
        if "log" in descriptor or re.search(uuid_matcher, descriptor) is not None:
            continue
        # descriptor = re.sub(uuid_matcher, "", descriptor)

        data_product = Dataproduct(cdf_metadata["instrument"], cdf_metadata["data_level"], descriptor)
        data_products[data_product][cdf_metadata["version"]].append(cdf_metadata["file_path"])

    print(data_products)

    index = []
    for data_product, versions_to_files in data_products.items():
        latest_version = max(versions_to_files.keys())
        latest_files = versions_to_files[latest_version]
        description_source_file = latest_files[0]

        description_source_file_url, cdf = get_cdf_file_from_sdc(description_source_file)
        cdf_file_info = CdfParser.parse_cdf_bytes(cdf, DefaultVariableSelector)

        date_in_url_path_regex = re.compile("/[0-9]{4}/[0-9]{2}/")
        date_in_file_name = re.compile("_[0-9]{8}_")

        template_url = re.sub(date_in_url_path_regex, "/%yyyy%/%mm%/", description_source_file_url)
        template_url = re.sub(date_in_file_name, "_%yyyymmdd%_", template_url)

        index.append(get_index_entry(cdf_file_info, template_url, description_source_file_url, [], data_product.instrument, "IMAP", DailyFileCadence))

    return index

def _capitalize_isois_instrument_name(instrument_name: str):
    return 'ISOIS' if instrument_name == 'isois' else instrument_name

if __name__ == '__main__':
    get_metadata_index()
