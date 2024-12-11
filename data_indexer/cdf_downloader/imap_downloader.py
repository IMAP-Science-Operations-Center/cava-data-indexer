import json
import ssl
import urllib.request
from typing import Any

import certifi

cdf_host = "http://3.139.73.210"
metadata_path = "/dev/science-files-metadata"

def get_all_metadata() -> [{}]:
    context = ssl.create_default_context(cafile=certifi.where())
    return json.loads(urllib.request.urlopen(cdf_host + "/dev/science-files-metadata", context=context).read())


def get_cdf_file(filename: str) -> dict:
    context = ssl.create_default_context(cafile=certifi.where())
    download_path = urllib.request.urlopen(cdf_host + "/dev/science-files-download?file=" + filename, context=context).read()
    link = cdf_host + download_path.decode("utf-8")

    return {"link": link, "data": urllib.request.urlopen(link, context=context).read()}

sdc_dev_server = "https://api.dev.imap-mission.com/"

def get_cdf_file_from_sdc(file_path: str) -> (str, Any):
    url = sdc_dev_server + "download/" + file_path
    return url, urllib.request.urlopen(url).read()
