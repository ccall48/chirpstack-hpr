import asyncio
import time
from math import ceil


def data_bytes_size(b64string: str) -> int:
    """DC for one uplink copy, estimated from the app payload (base64)."""
    bytes_per_dc = 24
    uplink_overhead = 13  # MHDR 1 + DevAddr 4 + FCtrl 1 + FCnt 2 + FPort 1 + MIC 4
    payload_len = (len(b64string) - b64string.count('=')) * 3 // 4
    frame_len = uplink_overhead + payload_len
    return max(ceil(frame_len / bytes_per_dc), 1)


def get_time():
    return int(time.time())


async def periodic_function(delay):
    # do something
    await asyncio.sleep(delay)
    asyncio.get_event_loop().call_later(delay, periodic_function)
