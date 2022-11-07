import json
import urllib.request

cdf_host = "http://3.139.73.210"


def get_all_metadata() -> [{}]:
    return json.loads(urllib.request.urlopen(cdf_host + "/dev/science-files-metadata").read())


def get_cdf_file(filename: str) -> bytes:
    download_path = urllib.request.urlopen(cdf_host + "/dev/science-files-download?file=" + filename).read()
    return urllib.request.urlopen(cdf_host + download_path.decode("utf-8")).read()
