from datetime import datetime, timezone

from data_indexer import utils
from data_indexer.cdf_downloader.psp_downloader import PspDownloader
from data_indexer.cdf_parser.cdf_parser import CdfParser
from data_indexer.http_client import get_with_retry
from data_indexer.utils import DataProductSource


class PspDataProcessor:

    @staticmethod
    def get_metadata_index():
        index = []

        psp_directory_infos = PspDownloader.get_all_metadata()

        for psp_directory_info in psp_directory_infos:
            for category, file_infos in psp_directory_info.file_infos_by_mode.items():

                file_urls = [PspDownloader.get_url(
                    psp_directory_info.base_url,
                    file_info.name,
                    psp_directory_info.instrument_url,
                    category,
                    file_info.year
                ) for file_info in file_infos]

                cdf_data = get_with_retry(file_urls[-1]).content
                cdf_info = CdfParser.parse_cdf_bytes(cdf_data, psp_directory_info.variable_selector)

                data_product_sources = []
                for url in file_urls:
                    file_name = url.split('/')[-1]
                    file_date = datetime.strptime(file_name.split('_')[-2], "%Y%m%d").replace(tzinfo=timezone.utc)
                    file_start, file_end = psp_directory_info.file_cadence.get_file_time_range(file_date)

                    data_product_sources.append(DataProductSource(start_time=file_start, end_time=file_end, url=url))

                index.append(utils.get_index_entry(
                    cdf_file_info=cdf_info,
                    file_timeranges=data_product_sources,
                    instrument=psp_directory_info.instrument_human_readable,
                    mission=psp_directory_info.mission,
                    file_cadence=psp_directory_info.file_cadence
                ))

        return index


if __name__ == '__main__':
    PspDataProcessor.get_metadata_index()
