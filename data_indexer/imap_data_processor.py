import re
import urllib
from collections import defaultdict
from dataclasses import dataclass
from functools import reduce
from typing import TypeVar

import imap_data_access
from spacepy.pycdf import CDFError

from data_indexer.cdf_parser.cdf_parser import CdfParser
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence
from data_indexer.utils import get_index_entry


@dataclass(frozen=True)
class Dataproduct:
    instrument: str
    data_level: str
    descriptor: str


T = TypeVar('T')


def flatten(l: list[list[T]]) -> list[T]:
    return reduce(list.__add__, l, [])


imap_dev_server = "https://api.dev.imap-mission.com/"

instrument_names = {
    "codice": "CoDICE",
    "glows": "GLOWS",
    "hi": "IMAP-Hi",
    "hit": "HIT",
    "idex": "IDEX",
    "lo": "IMAP-Lo",
    "mag": "MAG",
    "swapi": "SWAPI",
    "swe": "SWE",
    "ultra": "IMAP-Ultra",
}


def get_metadata_index() -> list[dict]:
    uuid_matcher = re.compile("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")

    l3_version_variants = ["l3", "l3a", "l3b"]
    l3_cdf_metadatas = flatten([imap_data_access.query(data_level=version) for version in l3_version_variants])

    data_products = defaultdict(lambda: defaultdict(list))
    for cdf_metadata in l3_cdf_metadatas:
        descriptor = cdf_metadata["descriptor"]
        if "log" in descriptor or re.search(uuid_matcher, descriptor) is not None:
            continue

        data_product = Dataproduct(cdf_metadata["instrument"], cdf_metadata["data_level"], descriptor)
        data_products[data_product][cdf_metadata["version"]].append(cdf_metadata["file_path"])

    index = []
    for data_product, versions_to_files in data_products.items():
        latest_version = max(versions_to_files.keys())
        latest_files = versions_to_files[latest_version]
        description_source_file = latest_files[0]

        source_file_url = imap_dev_server + "download/" + description_source_file
        cdf = urllib.request.urlopen(source_file_url).read()
        try:
            cdf_file_info = CdfParser.parse_cdf_bytes(cdf, DefaultVariableSelector)
        except:
            print("failed to parse CDF, skipping:", description_source_file)
            continue

        date_in_url_path_regex = re.compile("/[0-9]{4}/[0-9]{2}/")
        date_in_file_name = re.compile("_[0-9]{8}_")

        template_url = re.sub(date_in_url_path_regex, "/%yyyy%/%mm%/", source_file_url)
        template_url = re.sub(date_in_file_name, "_%yyyymmdd%_", template_url)

        index.append(get_index_entry(cdf_file_info, template_url, source_file_url, [],
                                     instrument_names.get(data_product.instrument, data_product.instrument), "IMAP",
                                     DailyFileCadence))

    return index


if __name__ == '__main__':
    get_metadata_index()
