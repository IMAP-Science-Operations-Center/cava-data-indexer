import json
import sys

from data_indexer import imap_data_processor
from data_indexer.psp_data_processor import PspDataProcessor

CURRENT_VERSION = "v2"


def main():
    args = sys.argv
    if args[1] == 'imap':
        with open(f'index_imap.{CURRENT_VERSION}.json', 'w') as file_handler:
            json.dump(imap_data_processor.get_metadata_index(), file_handler, indent=2)
    elif args[1] == 'psp':
        with open(f'index_psp.{CURRENT_VERSION}.json', 'w') as file_handler:
            json.dump(PspDataProcessor.get_metadata_index(), file_handler, indent=2)
    else:
        raise NotImplementedError("Unknown indexer requested")


if __name__ == "__main__":
    main()
