import json
import urllib.request

cdf_host = "http://3.139.73.210"

def get_all_metadata() -> [{}]:
    return json.loads(urllib.request.urlopen(cdf_host + "/dev/science-files-metadata").read())

def get_cdf_file(filename: str) -> bytes:
    return urllib.request.urlopen(cdf_host + "/dev/data-files-download?file=" + filename).read()