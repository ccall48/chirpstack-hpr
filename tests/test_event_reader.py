import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from event_reader import EventReader  # noqa: E402


class ReaderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.redis = AsyncMock()
        self.reader = EventReader(
            self.redis, ["api", "device"], "checkpoint", tail_streams=["device"]
        )

    async def test_first_start_skips_history_and_saves_cutover(self):
        self.redis.hget.return_value = None
        self.redis.xrevrange.return_value = [(b"100-0", {})]
        await self.reader.initialize()
        self.assertEqual(self.reader.offsets, {"api": "0-0", "device": b"100-0"})
        self.redis.xrevrange.assert_awaited_once_with("device", count=1)
        self.assertEqual(self.redis.hset.await_count, 2)

    async def test_restart_resumes_each_saved_stream(self):
        self.redis.hget.side_effect = [b"200-0", b"100-0"]
        await self.reader.initialize()
        self.assertEqual(self.reader.offsets, {"api": b"200-0", "device": b"100-0"})
        self.redis.xrevrange.assert_not_awaited()
        self.redis.hset.assert_not_awaited()

    async def test_all_returned_streams_have_independent_ids(self):
        self.redis.xread.return_value = [
            (b"api", [(b"200-0", {b"request": b"api"})]),
            (b"device", [(b"100-0", {b"up": b"uplink"})]),
        ]
        messages = await self.reader.read()
        self.assertEqual(
            messages,
            [
                ("api", (b"200-0", {b"request": b"api"})),
                ("device", (b"100-0", {b"up": b"uplink"})),
            ],
        )
        self.assertEqual(self.reader.offsets, {"api": "0-0", "device": "0-0"})
        for name, message in messages:
            await self.reader.acknowledge(name, message[0])
        self.assertEqual(self.reader.offsets, {"api": b"200-0", "device": b"100-0"})

    async def test_failed_checkpoint_retains_memory_offset(self):
        self.redis.hset.side_effect = ConnectionError("Redis unavailable")
        with self.assertRaises(ConnectionError):
            await self.reader.acknowledge("device", b"100-0")
        self.assertEqual(self.reader.offsets["device"], "0-0")

    async def test_disabled_publishing_needs_no_checkpoints(self):
        reader = EventReader(self.redis, ["api", "device"])
        await reader.initialize()
        self.redis.hget.assert_not_awaited()
        await reader.acknowledge("device", b"100-0")
        self.assertEqual(reader.offsets, {"api": "0-0", "device": b"100-0"})
        self.redis.hset.assert_not_awaited()
