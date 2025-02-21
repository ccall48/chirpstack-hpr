import time
from math import ceil


def data_bytes_size(b64string: str) -> int:
    """
    get byte count from a given base64 string from its length if 0 return 1.
    inital example: https://stackoverflow.com/a/6793638
    """
    dc = ceil(((len(b64string)*3)/4)/24)
    if dc == 0:
        return 1
    return dc


def get_time():
    return int(time.time())
