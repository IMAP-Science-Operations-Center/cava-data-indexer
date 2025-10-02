import time
from pathlib import Path
import imap_data_access


def get_with_retry(description_source_file, times: int = 5) -> Path:
    for i in range(times):
        try:
            return imap_data_access.download(description_source_file)
        except Exception as e:
            if i == times - 1:
                raise e
            print(f"Retrying get for url {description_source_file}; retry number {i+1}; exception {e}")
            time.sleep(2**i)