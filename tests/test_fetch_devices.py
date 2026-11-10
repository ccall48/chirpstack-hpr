import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import app


class FetchDevicesTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_concurrently_with_a_bounded_number_of_requests(self):
        entered = asyncio.Queue()
        release = asyncio.Event()
        active = peak = 0

        async def get(dev_eui, use_cache):
            nonlocal active, peak
            self.assertFalse(use_cache)
            active += 1
            peak = max(peak, active)
            entered.put_nowait(dev_eui)
            try:
                await release.wait()
                return {"devEui": dev_eui}, 1.0
            finally:
                active -= 1

        with patch.object(app, "DEVICE_FETCH_CONCURRENCY", 3), patch.object(
            app, "get_device_data", get
        ):
            task = asyncio.create_task(app.fetch_devices([str(i) for i in range(8)]))
            try:
                # Requests must overlap: none can finish before all three have started.
                for _ in range(3):
                    await asyncio.wait_for(entered.get(), 2)
                self.assertFalse(task.done())
                release.set()
                result = await asyncio.wait_for(task, 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(peak, 3)
        self.assertEqual([row[0] for row in result], [str(i) for i in range(8)])

    async def test_one_failure_preserves_successes_and_retries_on_next_pass(self):
        get = AsyncMock(side_effect=[
            ({"devEui": "good1"}, 1.0), RuntimeError("unavailable"),
            ({"devEui": "good2"}, 2.0), ({"devEui": "bad"}, 3.0),
        ])
        with patch.object(app, "get_device_data", get), patch("builtins.print"):
            result = await app.fetch_devices(["good1", "bad", "good2"])
            self.assertEqual(result, [
                ("good1", {"devEui": "good1"}, 1.0),
                ("good2", {"devEui": "good2"}, 2.0),
            ])
            self.assertEqual(get.await_count, 3)
            self.assertEqual(await app.fetch_devices(["bad"]), [
                ("bad", {"devEui": "bad"}, 3.0),
            ])

    async def test_cancellation_is_not_treated_as_a_failed_device(self):
        with patch.object(app, "get_device_data", AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await app.fetch_devices(["dev"])
