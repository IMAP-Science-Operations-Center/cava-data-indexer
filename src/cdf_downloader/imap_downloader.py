import json
import urllib.request

cdf_host = "http://3.139.73.210"
metadata_path = "/dev/science-files-metadata"


def get_all_metadata() -> [{}]:
    return json.loads(urllib.request.urlopen(cdf_host + "/dev/science-files-metadata").read())


def get_cdf_file(filename: str) -> dict:
    download_path = urllib.request.urlopen(cdf_host + "/dev/science-files-download?file=" + filename).read()
    link = cdf_host + download_path.decode("utf-8")
    return {"link": link, "data": urllib.request.urlopen(link).read()}
