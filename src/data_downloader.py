import urllib.request

cdf_host = "127.0.0.1:5000"

def get_all_metadata() -> str:
    return urllib.request.urlopen(cdf_host + "/dev/science-files-metadata").read()

def get_cdf_file(filename: str):
    return urllib.request.urlopen(cdf_host + "/dev/data-files-download?file=" + filename).read()