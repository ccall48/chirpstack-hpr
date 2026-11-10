import base64
import copy
import json
import os
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID

import boto3
from botocore.stub import Stubber

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from usage_publisher import (
    create_usage_publisher,
    publish_usage_event,
    uplink_dc_used,
)  # noqa: E402
from publishers.sqs_usage_publisher import SqsUsagePublisher  # noqa: E402


def uplink(size=25):
    return {
        "time": "2026-09-28T08:00:00Z",
        "deduplicationId": "uplink-uuid",
        "deviceInfo": {
            "devEui": "0102030405060708",
            "tenantId": "00000000-0000-0000-0000-000000000001",
            "applicationId": "00000000-0000-0000-0000-000000000002",
        },
        "data": base64.b64encode(bytes(size)).decode(),
        "rxInfo": [
            {"metadata": {"network": "helium_iot"}},
            {"metadata": {"network": "private"}},
            {"metadata": {"network": "helium_iot"}},
            {},
        ],
    }


class UsageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.publisher = SqsUsagePublisher(
            "https://sqs.eu-west-1.amazonaws.com/123/usage.fifo", "eu-west-1"
        )
        self.client = Mock()
        self.publisher._client = self.client

    def test_dc_boundaries_and_non_helium_receptions(self):
        for size, expected in [
            (0, 2),
            (1, 2),
            (23, 2),
            (24, 2),
            (25, 4),
            (48, 4),
            (49, 6),
        ]:
            with self.subTest(size=size):
                self.assertEqual(uplink_dc_used(uplink(size)), expected)
        event = uplink()
        del event["data"]
        self.assertEqual(uplink_dc_used(event), 2)

    async def test_v1_payload_and_fifo_contract(self):
        await publish_usage_event(self.publisher, uplink(), "123-0", "route")
        request = self.client.send_message.call_args.kwargs
        self.assertEqual(request["MessageGroupId"], "HeliumUsageEvents")
        self.assertEqual(len(request["MessageDeduplicationId"]), 64)
        self.assertEqual(
            json.loads(request["MessageBody"]),
            {
                "datetime": "2026-09-28T08:00:00",
                "dev_eui": "0102030405060708",
                "tenant_id": "00000000-0000-0000-0000-000000000001",
                "application_id": "00000000-0000-0000-0000-000000000002",
                "dc_used": 4,
            },
        )

    async def test_no_sqs_send_for_non_helium_uplink(self):
        event = uplink()
        event["rxInfo"] = [{}, {"metadata": {"network": "private"}}]
        await publish_usage_event(self.publisher, event, "123-0", "route")
        self.client.send_message.assert_not_called()

    async def test_retry_is_identical_and_new_uplink_is_distinct(self):
        event = uplink()
        await publish_usage_event(self.publisher, event, "123-0", "route")
        first = self.client.send_message.call_args
        await publish_usage_event(self.publisher, event, "123-0", "route")
        self.assertEqual(first, self.client.send_message.call_args)
        event["deduplicationId"] = "different-uplink"
        await publish_usage_event(self.publisher, event, "123-1", "route")
        self.assertNotEqual(
            first.kwargs["MessageDeduplicationId"],
            self.client.send_message.call_args.kwargs["MessageDeduplicationId"],
        )

    async def test_fallback_timestamp_and_identity_are_stable(self):
        event = uplink()
        del event["time"]
        del event["deduplicationId"]
        await publish_usage_event(self.publisher, event, "1000-0", "route")
        first = self.client.send_message.call_args
        self.assertEqual(
            json.loads(first.kwargs["MessageBody"])["datetime"], "1970-01-01T00:00:01"
        )
        await publish_usage_event(self.publisher, event, "1000-0", "route")
        self.assertEqual(first, self.client.send_message.call_args)
        await publish_usage_event(self.publisher, event, "1000-1", "route")
        self.assertNotEqual(first, self.client.send_message.call_args)

    async def test_uuid_identifiers_are_serialized(self):
        event = uplink()
        event["deviceInfo"]["tenantId"] = UUID(event["deviceInfo"]["tenantId"])
        await publish_usage_event(self.publisher, event, "123-0", "route")
        self.assertIsInstance(
            json.loads(self.client.send_message.call_args.kwargs["MessageBody"])[
                "tenant_id"
            ],
            str,
        )

    async def test_standard_queue_omits_fifo_fields(self):
        self.publisher.queue_url = self.publisher.queue_url.removesuffix(".fifo")
        await publish_usage_event(self.publisher, uplink(), "123-0", "route")
        self.assertEqual(
            set(self.client.send_message.call_args.kwargs), {"QueueUrl", "MessageBody"}
        )

    async def test_sdk_failure_propagates_to_caller(self):
        client = boto3.client(
            "sqs",
            region_name="eu-west-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        self.publisher._client = client
        with Stubber(client) as stub:
            stub.add_client_error("send_message", service_error_code="AccessDenied")
            with self.assertRaises(client.exceptions.ClientError):
                await publish_usage_event(self.publisher, uplink(), "123-0", "route")
            stub.assert_no_pending_responses()
        await self.publisher.close()

    async def test_client_reused_and_send_runs_outside_event_loop(self):
        self.publisher._client = None
        main_thread = threading.get_ident()
        threads = []
        self.client.send_message.side_effect = lambda **kwargs: threads.append(
            threading.get_ident()
        )
        with patch(
            "publishers.sqs_usage_publisher.boto3.client", return_value=self.client
        ) as factory:
            await publish_usage_event(self.publisher, uplink(), "123-0", "route")
            await publish_usage_event(self.publisher, uplink(), "123-1", "route")
            factory.assert_called_once()
        self.assertTrue(all(thread != main_thread for thread in threads))
        await self.publisher.close()
        self.client.close.assert_called_once()

    def test_configuration(self):
        with patch.dict(os.environ, {}, clear=True), patch(
            "publishers.sqs_usage_publisher.boto3.client"
        ) as factory:
            self.assertIsNone(create_usage_publisher())
            factory.assert_not_called()
        config = {
            "PUBLISH_USAGE_EVENTS": "True",
            "PUBLISH_USAGE_EVENTS_PROVIDER": "AWS_SQS",
            "PUBLISH_USAGE_EVENTS_SQS_URL": self.publisher.queue_url,
            "ROUTE_ID": "route",
        }
        with patch.dict(os.environ, config, clear=True):
            self.assertEqual(create_usage_publisher().region, "us-east-1")
        for key, value in [
            ("PUBLISH_USAGE_EVENTS_PROVIDER", "HTTP"),
            ("PUBLISH_USAGE_EVENTS_SQS_URL", ""),
            ("ROUTE_ID", ""),
        ]:
            invalid = copy.copy(config)
            invalid[key] = value
            with patch.dict(os.environ, invalid, clear=True), self.assertRaises(
                ValueError
            ):
                create_usage_publisher()

    async def test_normalized_event_can_be_sent_by_any_provider(self):
        provider = AsyncMock()
        await publish_usage_event(provider, uplink(), b"123-0", "route")
        event, event_id = provider.publish.await_args.args
        self.assertEqual(event["dc_used"], 4)
        self.assertEqual(event["dev_eui"], "0102030405060708")
        self.assertEqual(len(event_id), 64)
