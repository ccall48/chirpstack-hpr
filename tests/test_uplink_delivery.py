import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from chirpstack_api import integration
from google.protobuf.json_format import ParseDict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import app  # noqa: E402
from event_reader import EventReader  # noqa: E402
from publishers.sqs_usage_publisher import SqsUsagePublisher  # noqa: E402
from test_usage_publisher import uplink  # noqa: E402


class UplinkDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_sqs_send_drops_event_but_keeps_accounting_and_ack(self):
        # A billing-provider outage must not stall device/join sync behind it:
        # the usage event is dropped, everything else proceeds normally.
        payload = ParseDict(uplink(), integration.UplinkEvent()).SerializeToString()
        redis = AsyncMock()
        response = [(b"device:stream:event", [(b"1000-0", {b"up": payload})])]
        redis.xread.side_effect = [response, asyncio.CancelledError()]
        reader = EventReader(redis, ["device:stream:event"], "checkpoint")
        publisher = SqsUsagePublisher("https://example.com/usage.fifo", "eu-west-1")
        publisher._client = Mock()
        publisher._client.send_message.side_effect = RuntimeError("SQS unavailable")

        with patch.object(app, "database", AsyncMock()) as database, patch.object(
            app, "deviceredis", AsyncMock()
        ) as device_redis, patch("builtins.print"):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, Mock(), publisher)
            database.upsert_data_credits.assert_awaited_once_with(
                uplink()["deviceInfo"]["tenantId"],
                "",
                4,
            )
            device_redis.tenant_dc_stream.assert_awaited_once()
            redis.hset.assert_awaited_once_with(
                "checkpoint", "device:stream:event", b"1000-0"
            )
            publisher._client.send_message.assert_called_once()

    async def test_disabled_publishing_keeps_existing_accounting(self):
        payload = ParseDict(uplink(0), integration.UplinkEvent()).SerializeToString()
        reader = AsyncMock()
        reader.read.side_effect = [
            [("device", (b"1000-0", {b"up": payload}))],
            asyncio.CancelledError(),
        ]
        with patch.object(app, "database", AsyncMock()) as database, patch.object(
            app, "deviceredis", AsyncMock()
        ) as device_redis, patch(
            "publishers.sqs_usage_publisher.boto3.client"
        ) as client, patch(
            "builtins.print"
        ):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, Mock())
            database.upsert_data_credits.assert_awaited_once_with(
                uplink()["deviceInfo"]["tenantId"],
                "",
                2,
            )
            device_redis.tenant_dc_stream.assert_awaited_once()
            reader.acknowledge.assert_awaited_once_with("device", b"1000-0")
            client.assert_not_called()

    async def test_ignored_requests_are_acknowledged(self):
        reader = AsyncMock()
        reader.read.side_effect = [
            [
                ("api", (b"1000-0", {b"request": b'{"service": "inform"}'})),
                ("api", (b"1001-0", {b"request": b""})),
            ],
            asyncio.CancelledError(),
        ]
        with self.assertRaises(asyncio.CancelledError):
            await app.redis_events_streams(reader, Mock())
        reader.acknowledge.assert_any_await("api", b"1000-0")
        reader.acknowledge.assert_any_await("api", b"1001-0")

    def _join_device(self):
        return {
            "devEui": "0102030405060708",
            "name": "dev",
            "isDisabled": False,
            "variables": {},
            "tags": {},
            "joinEui": "0102030405060708",
            "devAddr": "01020304",
            "appSKey": "00" * 16,
            "nwkSEncKey": "00" * 16,
            "nwkKey": "00" * 16,
        }

    async def test_join_event_syncs_hpr_before_ack(self):
        device = self._join_device()
        payload = ParseDict(
            {"deviceInfo": {"devEui": device["devEui"]}}, integration.JoinEvent()
        ).SerializeToString()
        reader = AsyncMock()
        reader.read.side_effect = [
            [("device", (b"1000-0", {b"join": payload}))],
            asyncio.CancelledError(),
        ]
        hpr = AsyncMock()
        with patch.object(app, "database", AsyncMock()) as database, patch.object(
            app, "get_device_data", AsyncMock(return_value=device)
        ) as get_device_data, patch("builtins.print"):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, hpr)
            get_device_data.assert_awaited_once_with(device["devEui"], use_cache=False)
            database.upsert_device.assert_awaited_once()
            hpr.route_skfs.assert_awaited_once()
            reader.acknowledge.assert_awaited_once_with("device", b"1000-0")

    async def test_failed_join_sync_does_not_acknowledge(self):
        device = self._join_device()
        payload = ParseDict(
            {"deviceInfo": {"devEui": device["devEui"]}}, integration.JoinEvent()
        ).SerializeToString()
        reader = AsyncMock()
        reader.read.side_effect = [
            [("device", (b"1000-0", {b"join": payload}))],
            asyncio.CancelledError(),
        ]
        hpr = AsyncMock()
        hpr.route_skfs.side_effect = RuntimeError("helium rpc unavailable")
        with patch.object(app, "database", AsyncMock()), patch.object(
            app, "get_device_data", AsyncMock(return_value=device)
        ), patch("app.asyncio.sleep", new_callable=AsyncMock), patch("builtins.print"):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, hpr)
            reader.acknowledge.assert_not_awaited()
