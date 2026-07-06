import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone

from imap_data_access.file_validation import Version

from data_indexer.cdf_parser.cdf_parser import CdfParser
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.file_cadence.carrington_file_cadence import CarringtonFileCadence
from data_indexer.file_cadence.daily_file_cadence import DailyFileCadence
from data_indexer.file_cadence.map_file_cadence import BadFileNameError, MapFileCadence
from data_indexer.imap_data_access_utility import get_with_retry, query_chunked_data_product
from data_indexer.utils import DataProductSource, get_index_entry

logger = logging.getLogger()

@dataclass(frozen=True)
class Dataproduct:
    instrument: str
    data_level: str
    descriptor: str


INSTRUMENTS: list[str] = ["codice", "glows", "hi", "hit", "idex", "lo", "mag", "swapi", "swe", "ultra"]
L2_L3_DATA_LEVELS: list[str] = ["l2", "l2a", "l2b", "l2c", "l3", "l3a", "l3b", "l3c", "l3d", "l3e"]

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

    today: date = datetime.now(tz=timezone.utc).date()
    cdf_metadatas: list[dict] = query_chunked_data_product(instrument="mag", data_level="l1d", today=today)
    for instrument in INSTRUMENTS:
        for data_level in L2_L3_DATA_LEVELS:
            cdf_metadatas.extend(
                query_chunked_data_product(instrument=instrument, data_level=data_level, today=today)
            )

    data_products = defaultdict(lambda: dict())
    for cdf_metadata in cdf_metadatas:
        descriptor = cdf_metadata["descriptor"]
        if "log" in descriptor or re.search(uuid_matcher, descriptor) is not None:
            continue

        data_product = Dataproduct(cdf_metadata["instrument"], cdf_metadata["data_level"], descriptor)
        if data_product in data_products and cdf_metadata["start_date"] in data_products[data_product]:
            version = _get_version_from_metadata(cdf_metadata)
            existing_version = _get_version_from_metadata(data_products[data_product][cdf_metadata["start_date"]])
            if version is None or existing_version is None:
                continue
            if version > existing_version:
                data_products[data_product][cdf_metadata["start_date"]] = cdf_metadata
        else:
            data_products[data_product][cdf_metadata["start_date"]] = cdf_metadata

    index = []
    for data_product, dates_to_metadata in data_products.items():
        sorted_file_metadata = sorted(dates_to_metadata.values(), key=lambda x: x["start_date"])
        description_source_file = sorted_file_metadata[-1]["file_path"]

        cdf_path = get_with_retry(description_source_file)
        try:
            cdf_file_info = CdfParser.parse_cdf(cdf_path, DefaultVariableSelector)
        except Exception as e:
            print("failed to parse CDF, skipping:", description_source_file, e)
            continue

        try:
            data_product_sources = []
            for file_metadata in sorted_file_metadata:
                url = os.environ["IMAP_API_DOWNLOAD_URL"] + file_metadata["file_path"]
                start_time, end_time, cadence = determine_start_and_end_for_file(file_metadata)
                data_product_sources.append(DataProductSource(url=url, start_time=start_time, end_time=end_time))

            index.append(
                get_index_entry(cdf_file_info=cdf_file_info, file_timeranges=data_product_sources,
                                instrument=instrument_names.get(data_product.instrument, data_product.instrument),
                                mission="IMAP", file_cadence=cadence, data_level=data_product.data_level)
            )
        except BadFileNameError as e:
            print("failed to parse CDF, skipping:", description_source_file, e)
            continue

    return index


def determine_start_and_end_for_file(file_metadata):
    start_time = (datetime.strptime(file_metadata["start_date"], "%Y%m%d")).replace(tzinfo=timezone.utc)

    match file_metadata:
        case {"cr": cr} if cr is not None:
            cadence = CarringtonFileCadence()
            start_time, end_time = cadence.get_file_time_range_with_cr(cr)
        case {"instrument": "glows", "data_level": "l3b" | "l3c"}:
            cadence = CarringtonFileCadence()
            start_time, end_time = cadence.get_file_time_range(start_time)
        case {"instrument": "hi" | "lo" | "ultra"}:
            cadence = MapFileCadence(file_metadata["descriptor"].split("-")[-1])
            start_time, end_time = cadence.get_file_time_range(start_time)
        case _:
            cadence = DailyFileCadence()
            start_time, end_time = cadence.get_file_time_range(start_time)

    return start_time, end_time, cadence

def _get_version_from_metadata(metadata):
    version = None

    if "version" in metadata:
        version = Version.from_version(metadata["version"])
    elif "major_version" in metadata and "minor_version" in metadata:
        version = Version(metadata["major_version"], metadata["minor_version"])
    else:
        logger.error(f"Version format from data product does not match expected convention.\n Metadata: {metadata}")

    return version

if __name__ == "__main__":
    get_metadata_index()
