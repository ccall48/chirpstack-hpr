import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from chirpstack_api import integration, stream
from google.protobuf.json_format import ParseDict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import app  # noqa: E402
from event_reader import EventReader  # noqa: E402
from publishers.sqs_usage_publisher import SqsUsagePublisher  # noqa: E402
from tests.helpers import uplink  # noqa: E402


class UplinkDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def assert_no_handler_errors(self, output):
        errors = [
            call.args[0] for call in output.call_args_list
            if call.args and str(call.args[0]).startswith('[Error]:')
        ]
        self.assertEqual(errors, [], '\n'.join(errors))

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
        ) as device_redis, patch("builtins.print") as output, patch(
            "app.asyncio.sleep", new_callable=AsyncMock
        ):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, Mock(), publisher)
            self.assert_no_handler_errors(output)
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
        ) as output, patch("app.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, Mock())
            self.assert_no_handler_errors(output)
            database.upsert_data_credits.assert_awaited_once_with(
                uplink()["deviceInfo"]["tenantId"],
                "",
                2,
            )
            device_redis.tenant_dc_stream.assert_awaited_once()
            reader.acknowledge.assert_awaited_once_with("device", b"1000-0")
            client.assert_not_called()

    async def test_api_device_requests_dispatch_before_acknowledgement(self):
        # Cover JSON logging too: it must not prevent a device update from running.
        for method, handler in (
            ("Create", "add_device_euis"),
            ("Update", "update_device"),
            ("Delete", "remove_device_euis"),
        ):
            with self.subTest(method=method):
                metadata = {"dev_eui": "0102030405060708"}
                payload = ParseDict({
                    "service": "api.DeviceService", "method": method, "metadata": metadata,
                }, stream.ApiRequestLog()).SerializeToString()
                reader = AsyncMock()
                reader.read.side_effect = [
                    [("api", (b"1000-0", {b"request": payload}))],
                    asyncio.CancelledError(),
                ]
                hpr = AsyncMock()

                async def check_before_ack(*args):
                    reader.acknowledge.assert_not_awaited()

                getattr(hpr, handler).side_effect = check_before_ack
                with patch("builtins.print") as output, patch(
                    "app.asyncio.sleep", new_callable=AsyncMock
                ):
                    with self.assertRaises(asyncio.CancelledError):
                        await app.redis_events_streams(reader, hpr)
                    self.assert_no_handler_errors(output)
                getattr(hpr, handler).assert_awaited_once_with(metadata)
                reader.acknowledge.assert_awaited_once_with("api", b"1000-0")

    def _api_request(self, method, **fields):
        return ParseDict({
            "service": "api.DeviceService", "method": method, **fields,
        }, stream.ApiRequestLog()).SerializeToString()

    async def test_poison_message_is_skipped_after_retries_and_next_is_processed(self):
        poison = ("api", (b"1000-0", {b"request": self._api_request(
            "Create", metadata={"dev_eui": "0102030405060708"})}))
        reader = AsyncMock()
        reader.read.side_effect = [[poison]] * app.MAX_MESSAGE_ATTEMPTS + [
            [("api", (b"1001-0", {b"request": self._api_request(
                "Update", metadata={"dev_eui": "0102030405060708"})}))],
            asyncio.CancelledError(),
        ]
        hpr = AsyncMock()
        hpr.add_device_euis.side_effect = KeyError("dev_eui")
        with patch("app.asyncio.sleep", new_callable=AsyncMock), patch("builtins.print"):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, hpr)
        self.assertEqual(hpr.add_device_euis.await_count, app.MAX_MESSAGE_ATTEMPTS)
        hpr.update_device.assert_awaited_once()
        self.assertEqual(reader.acknowledge.await_args_list, [
            (("api", b"1000-0"),), (("api", b"1001-0"),),
        ])

    async def test_device_request_without_metadata_is_acknowledged(self):
        for method in ("Create", "Delete", "Update"):
            with self.subTest(method=method):
                reader = AsyncMock()
                reader.read.side_effect = [
                    [("api", (b"1000-0", {b"request": self._api_request(method)}))],
                    asyncio.CancelledError(),
                ]
                hpr = AsyncMock()
                with patch("builtins.print") as output:
                    with self.assertRaises(asyncio.CancelledError):
                        await app.redis_events_streams(reader, hpr)
                    self.assert_no_handler_errors(output)
                hpr.add_device_euis.assert_not_awaited()
                hpr.remove_device_euis.assert_not_awaited()
                hpr.update_device.assert_not_awaited()
                reader.acknowledge.assert_awaited_once_with("api", b"1000-0")

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
        async def check_before_ack(*args):
            reader.acknowledge.assert_not_awaited()

        hpr.sync_device.side_effect = check_before_ack
        with patch("builtins.print"):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, hpr)
        hpr.sync_device.assert_awaited_once_with(device["devEui"])
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
        hpr.sync_device.side_effect = RuntimeError("helium rpc unavailable")
        with patch("app.asyncio.sleep", new_callable=AsyncMock), patch("builtins.print"):
            with self.assertRaises(asyncio.CancelledError):
                await app.redis_events_streams(reader, hpr)
            reader.acknowledge.assert_not_awaited()
