import asyncio
import copy
import unittest
from unittest.mock import AsyncMock, patch

from tests.helpers import uplink
from chirpstack_api import stream
from google.protobuf.json_format import ParseDict
from redis.exceptions import ConnectionError

from helium_func import uplink_dc_used
from uplink_frames import get_uplink_frame_size


class UplinkFrameTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.event = uplink(11)
        self.event.update(devAddr='01020304', fCnt=65543, fPort=2)
        for index, rx in enumerate(self.event['rxInfo']):
            rx.update(gatewayId=f'{index + 1:016x}', uplinkId=100 + index)
        self.redis = AsyncMock()

    def frame(self, payload_size=11, fopts=1, port=2):
        # MHDR + DevAddr + FCtrl + FCnt + FOpts [+ FPort + payload] + MIC.
        raw = bytes([0x40]) + bytes.fromhex('04030201') + bytes([fopts]) + bytes([7, 0])
        raw += bytes(fopts)
        if port is not None:
            raw += bytes([port]) + bytes(payload_size)
        raw += bytes(4)
        return ParseDict({
            'devEui': self.event['deviceInfo']['devEui'],
            'rxInfo': self.event['rxInfo'],
        }, stream.UplinkFrameLog(phy_payload=raw))

    def entry(self, frame):
        return (b'999-0', {b'up': frame.SerializeToString()})

    async def test_fopts_changes_dc_at_boundary(self):
        self.redis.xrevrange.return_value = [self.entry(self.frame())]
        size = await get_uplink_frame_size(self.redis, self.event, b'1000-0')
        self.assertEqual(size, 25)
        self.assertEqual(uplink_dc_used(self.event), 2)
        self.assertEqual(uplink_dc_used(self.event | {'phyPayloadByteCount': size}), 4)
        self.redis.xrevrange.assert_awaited_once_with(
            'device:{0102030405060708}:stream:frame', max='1000-18446744073709551615', count=100
        )

    async def test_matches_original_receptions_not_latest_packet(self):
        matching = self.frame()
        wrong = copy.deepcopy(matching)
        wrong.rx_info[0].uplink_id += 1
        self.redis.xrevrange.return_value = [self.entry(wrong), self.entry(matching)]
        self.assertEqual(await get_uplink_frame_size(self.redis, self.event, '1000-0'), 25)

    async def test_wrong_device_address_counter_port_or_message_type_is_rejected(self):
        for field, value in [('devAddr', 'aabbccdd'), ('fCnt', 8), ('fPort', 3), ('confirmed', True)]:
            with self.subTest(field=field):
                self.redis.xrevrange.return_value = [self.entry(self.frame())]
                self.assertIsNone(await get_uplink_frame_size(
                    self.redis, self.event | {field: value}, '1000-0'
                ))
        wrong = self.frame()
        wrong.dev_eui = 'ffffffffffffffff'
        self.redis.xrevrange.return_value = [self.entry(wrong)]
        self.assertIsNone(await get_uplink_frame_size(self.redis, self.event, '1000-0'))

    async def test_confirmed_uplink(self):
        frame = self.frame()
        frame.phy_payload = bytes([0x80]) + frame.phy_payload[1:]
        self.redis.xrevrange.return_value = [self.entry(frame)]
        self.assertEqual(await get_uplink_frame_size(
            self.redis, self.event | {'confirmed': True}, '1000-0'
        ), 25)

    async def test_mac_only_and_empty_frames_use_actual_length(self):
        event = self.event | {'data': '', 'fPort': 0}
        for frame, size, dc in [(self.frame(0, 0, None), 12, 2),
                                (self.frame(0, 13, None), 25, 4),
                                (self.frame(15, 0, 0), 28, 4)]:
            with self.subTest(size=size):
                self.redis.xrevrange.return_value = [self.entry(frame)]
                measured = await get_uplink_frame_size(self.redis, event, '1000-0')
                self.assertEqual(measured, size)
                self.assertEqual(uplink_dc_used(event | {'phyPayloadByteCount': measured}), dc)

    async def test_expired_or_disabled_frame_history_returns_no_size(self):
        self.redis.xrevrange.return_value = []
        self.assertIsNone(await get_uplink_frame_size(self.redis, self.event, '1000-0'))

    async def test_missing_reception_identity_does_not_guess(self):
        event = copy.deepcopy(self.event)
        del event['rxInfo'][0]['uplinkId']
        self.assertIsNone(await get_uplink_frame_size(self.redis, event, '1000-0'))
        self.redis.xrevrange.assert_not_awaited()

    async def test_corrupt_and_downlink_entries_do_not_hide_valid_frame(self):
        broken = self.frame()
        broken.phy_payload = b'bad'
        self.redis.xrevrange.return_value = [
            (b'999-3', {b'down': b'ignored'}),
            (b'999-2', {b'up': b'\xff'}),
            self.entry(broken), self.entry(self.frame()),
        ]
        self.assertEqual(await get_uplink_frame_size(self.redis, self.event, '1000-0'), 25)

    async def test_redis_outage_returns_no_size(self):
        self.redis.xrevrange.side_effect = ConnectionError('unavailable')
        with patch('builtins.print'):
            self.assertIsNone(await get_uplink_frame_size(self.redis, self.event, '1000-0'))

    async def test_stalled_lookup_is_cancelled_and_falls_back(self):
        cancelled = asyncio.Event()

        async def stalled(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        self.redis.xrevrange.side_effect = stalled
        with patch('uplink_frames.FRAME_LOOKUP_TIMEOUT', 0.01), patch('builtins.print'):
            self.assertIsNone(await get_uplink_frame_size(self.redis, self.event, '1000-0'))
        self.assertTrue(cancelled.is_set())

    async def test_shutdown_cancellation_is_not_swallowed(self):
        self.redis.xrevrange.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await get_uplink_frame_size(self.redis, self.event, '1000-0')

    def test_invalid_measured_size_is_not_silently_billed(self):
        for value in [0, 11, -1, True, '25', 25.5]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                uplink_dc_used(self.event | {'phyPayloadByteCount': value})
