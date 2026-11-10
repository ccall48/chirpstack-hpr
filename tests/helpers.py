import asyncio
import base64
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from HeliumProtos import HeliumConfigCli
from models import DeviceDatabase


def device(dev_eui="0102030405060708", key="11" * 16):
    return {
        "devEui": dev_eui,
        "name": "dev",
        "isDisabled": False,
        "variables": {},
        "tags": {},
        "joinEui": "0102030405060708",
        "devAddr": "01020304",
        "appSKey": "22" * 16,
        "nwkSEncKey": key,
        "nwkKey": "33" * 16,
    }


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


class DatabaseTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = DeviceDatabase()
        self.db.database = str(Path(directory.name) / "devices.db")
        self.addAsyncCleanup(self.db.close_pool)
        await self.db.create_tables()
        # Exercise real synchronization methods without loading delegate credentials.
        self.hpr = object.__new__(HeliumConfigCli)
        self.hpr.database = self.db
        self.hpr.route_id = "test-route"
        self.hpr.sync_lock = asyncio.Lock()
        self.hpr.route_skfs = AsyncMock()
        self.hpr.route_euis = AsyncMock()

    async def stored_key(self, dev_eui="0102030405060708"):
        async with self.db.pool.connection() as conn:
            async with conn.execute(
                "SELECT nwkSEncKey FROM devices WHERE devEui = ?",
                (str(int(dev_eui, 16)),),
            ) as cursor:
                row = await cursor.fetchone()
        return None if row is None else row[0]

    async def seed_skfs(self, keys, route_id="test-route"):
        await self.db.upsert_helium_skfs([
            {"routeId": route_id, "devaddr": "16909060", "sessionKey": key, "maxCopies": 3}
            for key in keys
        ])

    async def stored_skfs(self, route_id="test-route"):
        async with self.db.pool.connection() as conn:
            async with conn.execute(
                "SELECT sessionKey FROM helium_skfs WHERE routeId = ?", (route_id,)
            ) as cursor:
                return {row[0] for row in await cursor.fetchall()}

    async def cancel_task(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def start_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.addAsyncCleanup(self.cancel_task, task)
        return task
