import sqlite3
from unittest.mock import patch

from tests.helpers import DatabaseTestCase, device
from models import device_row
from schemas import GetDeviceSyncRequest


def row(key, observed_at=1.0):
    return device_row(GetDeviceSyncRequest(**device(key=key)), observed_at, "test-route")


class DeviceDatabaseTests(DatabaseTestCase):
    async def test_failed_batch_rolls_back_earlier_updates_and_pool_remains_usable(self):
        await self.db.upsert_device([row("old-key")])
        bad_row = row("bad-key")
        bad_row["devEui"] = "123"
        bad_row["joinEui"] = None
        with patch("builtins.print"), self.assertRaises(sqlite3.IntegrityError):
            await self.db.upsert_device([row("new-key"), bad_row])
        self.assertEqual(await self.stored_key(), "old-key")
        await self.db.upsert_device([row("recovered-key")])
        self.assertEqual(await self.stored_key(), "recovered-key")

    async def test_rotation_and_confirmed_deactivation_replace_stored_key(self):
        await self.db.upsert_device([row("old-key")])
        await self.db.upsert_device([row("rotated-key", 2.0)])
        self.assertEqual(await self.stored_key(), "rotated-key")
        data = device()
        for field in ("nwkSEncKey", "nwkKey", "appSKey", "devAddr"):
            del data[field]
        await self.db.upsert_device([
            device_row(GetDeviceSyncRequest(**data), 3.0, "test-route")
        ])
        self.assertEqual(await self.stored_key(), "")

    async def test_stale_selection_is_read_only_and_scoped_to_route(self):
        await self.db.upsert_device([row("active")])
        await self.seed_skfs(["active", "stale"])
        await self.seed_skfs(["other-key"], "other-route")
        candidates = await self.db.get_stale_skfs("test-route")
        self.assertEqual([r["sessionKey"] for r in candidates], ["stale"])
        self.assertEqual(await self.stored_skfs(), {"active", "stale"})
        await self.db.delete_helium_skfs("test-route", ["stale", "other-key"])
        self.assertEqual(await self.stored_skfs(), {"active"})
        self.assertEqual(await self.stored_skfs("other-route"), {"other-key"})

    async def test_reverification_removes_nothing_when_database_read_fails(self):
        async with self.db.pool.connection() as conn:
            await conn.execute("DROP TABLE devices")
            await conn.commit()
        with patch("builtins.print"):
            self.assertEqual(await self.db.filter_still_stale(["unknown"]), [])

    async def test_existing_database_migration_preserves_devices_and_is_repeatable(self):
        await self.db.upsert_device([row("existing")])
        # Use the old schema, including its constraints, to exercise an actual upgrade.
        async with self.db.pool.connection() as conn:
            await conn.execute("ALTER TABLE devices RENAME TO current_devices")
            await conn.execute("""
                CREATE TABLE devices (
                    devEui TEXT PRIMARY KEY, name TEXT, isDisabled BOOLEAN NOT NULL,
                    variables TEXT, tags TEXT, joinEui TEXT NOT NULL, devAddr TEXT,
                    nwkKey TEXT, appSKey TEXT, nwkSEncKey TEXT, routeId TEXT
                )
            """)
            await conn.execute("INSERT INTO devices SELECT devEui, name, isDisabled, "
                               "variables, tags, joinEui, devAddr, nwkKey, appSKey, "
                               "nwkSEncKey, routeId FROM current_devices")
            await conn.execute("DROP TABLE current_devices")
            await conn.commit()
        await self.db.create_tables()
        await self.db.create_tables()
        self.assertEqual(await self.stored_key(), "existing")
        async with self.db.pool.connection() as conn:
            async with conn.execute("SELECT keyFetchedAt FROM devices") as cursor:
                self.assertEqual((await cursor.fetchone())[0], 0)
        await self.db.upsert_device([row("rotated-after-migration")])
        self.assertEqual(await self.stored_key(), "rotated-after-migration")
