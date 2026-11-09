import asyncio
import base64
import time
from math import ceil


def data_bytes_size(b64string: str) -> int:
    """
    DC per copy: Helium bills ceil(max(frame_bytes, 24) / 24) on the full LoRaWAN frame.
    frame = MHDR(1) + DevAddr(4) + FCtrl(1) + FCnt(2) + MIC(4) = 12, + FPort(1) + payload when there is data.
    FOpts isn't in the uplink event, so this is a lower bound.
    """
    payload_bytes = len(base64.b64decode(b64string or ''))
    frame_bytes = payload_bytes + 13 if payload_bytes else 12
    return ceil(max(frame_bytes, 24) / 24)


def uplink_dc_used(uplink):
    hotspots = sum(
        gw.get('metadata', {}).get('network') == 'helium_iot'
        for gw in uplink.get('rxInfo', [])
    )
    frame_bytes = uplink.get('phyPayloadByteCount')
    if frame_bytes is not None:
        if type(frame_bytes) is not int or frame_bytes < 12:
            raise ValueError('phyPayloadByteCount must be a full LoRaWAN frame length (at least 12 bytes)')
        return ((frame_bytes + 23) // 24) * hotspots
    return data_bytes_size(uplink.get('data', '')) * hotspots


def get_time():
    return int(time.time())


async def periodic_function(delay):
    # do something
    await asyncio.sleep(delay)
    asyncio.get_event_loop().call_later(delay, periodic_function)
