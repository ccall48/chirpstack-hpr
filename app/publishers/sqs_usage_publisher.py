import asyncio
import json

import boto3
from botocore.config import Config


class SqsUsagePublisher:
    def __init__(self, queue_url, region):
        self.queue_url = queue_url
        self.region = region
        self._client = None

    async def publish(self, event, event_id):
        # boto3 blocks; keep its network calls off the asyncio event loop.
        await asyncio.to_thread(self._send, event, event_id)

    def _send(self, event, event_id):
        if self._client is None:
            self._client = boto3.client(
                "sqs",
                region_name=self.region,
                config=Config(
                    connect_timeout=5,
                    read_timeout=10,
                    retries={"mode": "standard", "total_max_attempts": 3},
                ),
            )
        request = {"QueueUrl": self.queue_url, "MessageBody": json.dumps(event)}
        if self.queue_url.endswith(".fifo"):
            request.update(
                MessageGroupId="HeliumUsageEvents", MessageDeduplicationId=event_id
            )
        # Let failures propagate; the caller decides whether to retry, drop, or log.
        self._client.send_message(**request)

    async def close(self):
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
