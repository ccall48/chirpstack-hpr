import asyncio
import time
from math import ceil


def data_bytes_size(b64string: str) -> int:
    """
    get byte count from a given base64 string from its length if 0 return 1.
    inital example: https://stackoverflow.com/a/6793638
    """
    helium_bytes_per_dc = 24
    # dc = ceil(((len(b64string)*3)/4)/24)
    dc_used = ceil(((len(b64string) - b64string.count('='))*3//4)/helium_bytes_per_dc)
    if dc_used == 0:
        # we still get chatged per uplink if msg <= 0 bytes
        return 1
    return dc_used


def get_time():
    return int(time.time())


async def periodic_function(delay):
    # do something
    await asyncio.sleep(delay)
    asyncio.get_event_loop().call_later(delay, periodic_function)
