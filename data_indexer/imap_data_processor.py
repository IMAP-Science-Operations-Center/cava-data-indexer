import re
import urllib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import reduce
from typing import TypeVar

import imap_data_access

from data_indexer.cdf_parser.cdf_parser import CdfParser
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence
from data_indexer.http_client import get_with_retry
from data_indexer.utils import get_index_entry, DataProductSource


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

    l3_data_level_variants = ["l3", "l3a", "l3b", "l3c", "l3d", "l3e"]
    l3_cdf_metadatas = flatten([imap_data_access.query(data_level=data_level) for data_level in l3_data_level_variants])

    data_products = defaultdict(dict)
    for cdf_metadata in l3_cdf_metadatas:
        descriptor = cdf_metadata["descriptor"]
        if "log" in descriptor or re.search(uuid_matcher, descriptor) is not None:
            continue

        data_product = Dataproduct(cdf_metadata["instrument"], cdf_metadata["data_level"], descriptor)
        if data_product in data_products and cdf_metadata['start_date'] in data_products[data_product]:
            if cdf_metadata['version'] > data_products[data_product][cdf_metadata["start_date"]]['version']:
                data_products[data_product][cdf_metadata["start_date"]] = cdf_metadata
        else:
            data_products[data_product][cdf_metadata['start_date']] = cdf_metadata

    index = []
    for data_product, dates_to_metadata in data_products.items():
        sorted_dates = sorted(dates_to_metadata.values(), key=lambda x: x['start_date'])
        description_source_file = sorted_dates[-1]['file_path']

        source_file_url = imap_dev_server + "download/" + description_source_file
        cdf = get_with_retry(source_file_url).content
        try:
            cdf_file_info = CdfParser.parse_cdf_bytes(cdf, DefaultVariableSelector)
        except Exception as e:
            print("failed to parse CDF, skipping:", description_source_file, e)
            continue

        data_product_sources = []
        for available_date in sorted_dates:
            start_time = (datetime.strptime(available_date['start_date'], '%Y%m%d')).replace(tzinfo=timezone.utc)
            end_time = start_time + timedelta(days=1)
            url = imap_dev_server + "download/" + available_date['file_path']
            data_product_sources.append(DataProductSource(url=url,
                                                          start_time=start_time,
                                                          end_time=end_time))

        index.append(get_index_entry(cdf_file_info=cdf_file_info,
                                     file_timeranges=data_product_sources,
                                     instrument=instrument_names.get(data_product.instrument, data_product.instrument),
                                     mission="IMAP",
                                     file_cadence=DailyFileCadence))

    return index


if __name__ == '__main__':
    get_metadata_index()
