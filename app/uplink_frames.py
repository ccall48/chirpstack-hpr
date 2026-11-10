import asyncio
import re

from chirpstack_api import stream
from google.protobuf.message import DecodeError
from redis.exceptions import RedisError


FRAME_LOOKUP_TIMEOUT = 2


async def get_uplink_frame_size(redis, uplink, message_id):
    """Return the matching full frame length, or None when it cannot be verified.

    ChirpStack writes this log before publishing the integration event. Restrict
    the lookup to the event's Redis timestamp, then match the device, DevAddr,
    FCnt and gateway reception IDs; never borrow the latest frame's size blindly.
    """
    dev_eui = uplink.get('deviceInfo', {}).get('devEui', '').lower()
    dev_addr = uplink.get('devAddr', '').lower()
    if not re.fullmatch(r'[0-9a-f]{16}', dev_eui) or not re.fullmatch(r'[0-9a-f]{8}', dev_addr):
        return None
    receptions = {
        (rx['gatewayId'], rx['uplinkId'])
        for rx in uplink.get('rxInfo', []) if rx.get('gatewayId') and 'uplinkId' in rx
    }
    if not receptions or len(receptions) != len(uplink.get('rxInfo', [])):
        return None
    if isinstance(message_id, bytes):
        message_id = message_id.decode()
    event_ms = int(message_id.split('-')[0])
    # Redis sequence numbers are independent between streams; include all entries
    # in the same millisecond as the integration event.
    try:
        frames = await asyncio.wait_for(
            redis.xrevrange(
                f'device:{{{dev_eui}}}:stream:frame',
                max=f'{event_ms}-18446744073709551615', count=100,
            ),
            timeout=FRAME_LOOKUP_TIMEOUT,
        )
    except (RedisError, TimeoutError) as exc:
        print(f'[Frame lookup failed]: {dev_eui}: {exc}')
        return None

    for _, fields in frames:
        if b'up' not in fields:
            continue
        frame = stream.UplinkFrameLog()
        try:
            frame.ParseFromString(fields[b'up'])
        except DecodeError:
            continue
        if frame.dev_eui.lower() != dev_eui:
            continue
        if {(rx.gateway_id, rx.uplink_id) for rx in frame.rx_info} != receptions:
            continue
        raw = frame.phy_payload
        if len(raw) < 12:
            continue
        expected_type = 4 if uplink.get('confirmed', False) else 2
        if raw[0] >> 5 != expected_type:
            continue
        if f'{int.from_bytes(raw[1:5], "little"):08x}' != dev_addr:
            continue
        if int.from_bytes(raw[6:8], 'little') != (uplink.get('fCnt', 0) & 0xffff):
            continue
        # FHDR ends after its optional FOpts. FPort is absent for an empty frame.
        port_offset = 8 + (raw[5] & 0x0f)
        if len(raw) < port_offset + 4:
            continue
        port = raw[port_offset] if len(raw) > port_offset + 4 else 0
        if port != uplink.get('fPort', 0):
            continue
        return len(raw)
    return None
