import os
import json
import aiosqlite
from aiosqlitepool import SQLiteConnectionPool


def device_row(device, observed_at, route_id):
    return {
        'devEui': str(device.devEui),
        'name': device.name,
        'isDisabled': device.isDisabled,
        'variables': json.dumps(device.variables),
        'tags': json.dumps(device.tags),
        'joinEui': str(device.joinEui),
        'devAddr': str(device.devAddr or 0),
        'nwkKey': device.nwkKey or '',
        'appSKey': device.appSKey or '',
        'nwkSEncKey': device.nwkSEncKey or '',
        'route_id': route_id,
        'keyFetchedAt': observed_at,
    }


class DeviceDatabase:
    def __init__(self):
        self.database = os.getenv('SQLITE_DATABASE_NAME', 'chirpstack-hpr-v2.db')
        self.pool: SQLiteConnectionPool = None


    async def init_pool(self, pool_size: int = 10):
        """Initialize the pool once at application startup."""
        if self.pool is None:
            async def connection_factory():
                conn = await aiosqlite.connect(self.database)
                await conn.execute("PRAGMA foreign_keys = ON")
                return conn

            self.pool = SQLiteConnectionPool(
                connection_factory=connection_factory,
                pool_size=pool_size
            )


    async def close_pool(self):
        """Cleanup pool at application shutdown."""
        if self.pool:
            await self.pool.close()
            self.pool = None


    async def create_tables(self):
        if not self.pool:
            await self.init_pool()

        try:
            async with self.pool.connection() as db:
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS devices (
                        devEui TEXT PRIMARY KEY,
                        name TEXT,
                        isDisabled BOOLEAN NOT NULL,
                        variables TEXT,
                        tags TEXT,
                        joinEui TEXT NOT NULL,
                        devAddr TEXT,
                        nwkKey TEXT,
                        appSKey TEXT,
                        nwkSEncKey TEXT,
                        routeId TEXT,
                        keyFetchedAt REAL NOT NULL DEFAULT 0
                )""")
                async with db.execute("PRAGMA table_info(devices)") as cursor:
                    columns = {row[1] async for row in cursor}
                if 'keyFetchedAt' not in columns:
                    await db.execute("ALTER TABLE devices ADD COLUMN keyFetchedAt REAL NOT NULL DEFAULT 0")
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS data_credits (
                        tenantId TEXT PRIMARY KEY,
                        tenantName TEXT,
                        dc_balance INTEGER default 0,
                        dc_used INTEGER,
                        dc_multiplier INTEGER default 3
                )""")
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        id INTEGER PRIMARY KEY,
                        tenantId TEXT,
                        txid TEXT,
                        amount TEXT
                )""")
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS helium_skfs (
                        routeId TEXT NOT NULL,
                        devaddr TEXT NOT NULL,
                        sessionKey TEXT UNIQUE NOT NULL,
                        maxCopies INTEGER DEFAULT 0,
                        --
                        UNIQUE (routeId, devaddr, sessionKey)
                )""")
                await db.commit()
        except aiosqlite.Error as e:
            print('[SQL Error: create_tables]\n', e)
            raise


    async def upsert_device(self, kwargs):
        if not self.pool:
            await self.init_pool()

        # All device-key writers hold the shared sync lock from fetch to commit.
        # Local timestamps are metadata, not a version supplied by ChirpStack.
        sql = """
            INSERT INTO devices
            (devEui, name, isDisabled, variables, tags, joinEui, devAddr, nwkKey, appSKey, nwkSEncKey, routeId, keyFetchedAt)
            VALUES (
                :devEui, :name, :isDisabled, :variables, :tags,
                :joinEui, :devAddr, :nwkKey, :appSKey, :nwkSEncKey, :route_id, :keyFetchedAt
            )
            ON CONFLICT(devEui) DO UPDATE
            SET name=:name,
                isDisabled=:isDisabled,
                variables=:variables,
                tags=:tags,
                joinEui=:joinEui,
                routeId=:route_id,
                devAddr=excluded.devAddr,
                nwkKey=excluded.nwkKey,
                appSKey=excluded.appSKey,
                nwkSEncKey=excluded.nwkSEncKey,
                keyFetchedAt=excluded.keyFetchedAt
            """
        try:
            async with self.pool.connection() as db:
                try:
                    await db.executemany(sql, kwargs)
                    await db.commit()
                except BaseException:
                    await db.rollback()
                    raise
        except aiosqlite.Error as e:
            print('[SQL ERROR: upsert_device]\n', e)
            raise


    async def upsert_data_credits(self, tenantId, tenantName, dc_used):
        if not self.pool:
            await self.init_pool()

        sql = """
            INSERT INTO data_credits
            (tenantId, tenantName, dc_used)
            VALUES (:tenantId, :tenantName, :dc_used)
            ON CONFLICT (tenantId) DO UPDATE
            SET tenantName=:tenantName,
                dc_balance = dc_balance - (dc_multiplier * :dc_used),
                dc_used = dc_used + :dc_used
        """
        try:
            async with self.pool.connection() as db:
                await db.execute(sql, (tenantId, tenantName, dc_used,))
                await db.commit()
        except aiosqlite.Error as e:
            print('[SQL ERROR: upsert_data_credits]\n', e)
            await db.rollback()


    async def upsert_helium_skfs(self, kwargs):
        if not self.pool:
            await self.init_pool()

        sql = """
            INSERT INTO helium_skfs
            (routeId, devaddr, sessionKey, maxCopies)
            VALUES (:routeId, :devaddr, :sessionKey, :maxCopies)
            -- ON CONFLICT DO NOTHING --
            ON CONFLICT (routeId, devaddr, sessionKey)
            DO UPDATE SET maxCopies = EXCLUDED.maxCopies
            WHERE helium_skfs.maxCopies IS DISTINCT FROM EXCLUDED.maxCopies;
        """
        try:
            async with self.pool.connection() as db:
                await db.executemany(sql, kwargs)
                await db.commit()
        except aiosqlite.Error as e:
            print('[SQL ERROR: upsert_helium_skfs]\n', e)
            await db.rollback()


    async def get_device_euis(self, dev_eui):
        if not self.pool:
            await self.init_pool()

        sql = f"""
            SELECT devEui, joinEui
            FROM devices
            WHERE devEui = '{int(dev_eui, 16)}';
        """
        try:
            async with self.pool.connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(sql) as cursor:
                    row = await cursor.fetchone()
                return int(row['devEui']), int(row['joinEui'])
        except aiosqlite.Error as e:
            print('[SQL Error get_device_euis]\n', e)
            await db.rollback()


    # # # # # # # # # #
    # Purge old device session keys from helium packet router
    # # # # #
    async def get_stale_skfs(self, route_id):
        if not self.pool:
            await self.init_pool()
        # Candidate selection must not discard retry state before Helium confirms.
        sql = """
            SELECT * FROM helium_skfs
            WHERE routeId = ? AND sessionKey NOT IN (SELECT nwkSEncKey FROM devices)
        """
        async with self.pool.connection() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(sql, (route_id,)) as cursor:
                return await cursor.fetchall()

    async def delete_helium_skfs(self, route_id, session_keys):
        if not self.pool:
            await self.init_pool()
        async with self.pool.connection() as db:
            try:
                await db.executemany(
                    "DELETE FROM helium_skfs WHERE routeId = ? AND sessionKey = ?",
                    [(route_id, key) for key in session_keys],
                )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def filter_still_stale(self, session_keys: list[str]) -> list[str]:
        # Caller holds the shared sync lock through this check and the Helium RPC.
        if not session_keys:
            return []
        if not self.pool:
            await self.init_pool()

        placeholders = ','.join('?' * len(session_keys))
        sql = f"SELECT nwkSEncKey FROM devices WHERE nwkSEncKey IN ({placeholders})"
        try:
            async with self.pool.connection() as db:
                async with db.execute(sql, session_keys) as cursor:
                    now_in_use = {row[0] async for row in cursor}
            return [key for key in session_keys if key not in now_in_use]
        except aiosqlite.Error as e:
            print('[SQL Error: filter_still_stale]\n', e)
            return []  # be conservative: remove nothing if we can't verify it's still stale


"""
devEui=3240324265253275232
name='T1000A-iZincit'
isDisabled=False
variables={'max_copies': 100, 'private': False}
tags={'max_copies': 100, 'private': False}
joinEui=16469707286779846324
devAddr=2013266407
appSKey='aceef1dd3c10bde78dc2a4f966d990e2'
nwkSEncKey='e5e9c6b47880087d9b3a5f21b495031d'


CREATE TABLE IF NOT EXISTS devices (
    devEui INTEGER PRIMARY KEY,     -- 3240324265253275232
    name TEXT,                      -- 'T1000A-iZincit'
    isDisabled NUMERIC NOT NULL,    -- False
    variables TEXT,                 -- {'max_copies': 100, 'private': False}
    tags TEXT,                      -- {}
    joinEui INTEGER NOT NULL,       -- 16469707286779846324
    devAddr INTEGER NOT NULL,       -- 2013266407
    appSKey TEXT,                   -- 'aceef1dd3c10bde78dc2a4f966d990e2'
    nwkSEncKey TEXT                 -- 'e5e9c6b47880087d9b3a5f21b495031d'
)
"""
