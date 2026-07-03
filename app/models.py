import os
import aiosqlite
from aiosqlitepool import SQLiteConnectionPool


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
                        routeId TEXT
                )""")
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
            await db.rollback()


    async def upsert_device(self, kwargs):
        if not self.pool:
            await self.init_pool()

        sql = """
            INSERT INTO devices
            (devEui, name, isDisabled, variables, tags, joinEui, devAddr, nwkKey, appSKey, nwkSEncKey, routeId)
            VALUES (:devEui, :name, :isDisabled, :variables, :tags, :joinEui, :devAddr, :nwkKey, :appSKey, :nwkSEncKey, :route_id)
            ON CONFLICT(devEui) DO UPDATE
            SET name=:name,
                isDisabled=:isDisabled,
                variables=:variables,
                tags=:tags,
                joinEui=:joinEui,
                devAddr=:devAddr,
                nwkKey=:nwkKey,
                appSKey=:appSKey,
                nwkSEncKey=:nwkSEncKey,
                routeId=:route_id
            """
        try:
            async with self.pool.connection() as db:
                await db.executemany(sql, kwargs)
                await db.commit()
        except aiosqlite.Error as e:
            print('[SQL ERROR: upsert_device]\n', e)
            await db.rollback()


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
    async def get_stale_skfs(self):
        if not self.pool:
            await self.init_pool()

        sql = """
            SELECT * FROM helium_skfs
            WHERE sessionKey NOT IN (SELECT nwkSEncKey FROM devices);
        """
        rm_sql = """
            DELETE FROM helium_skfs
            WHERE sessionKey NOT IN (SELECT nwkSEncKey FROM devices);
        """
        try:
            skfs_to_remove = []
            async with self.pool.connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(sql) as cursor:
                    async for row in cursor:
                        skfs_to_remove.append(row)
                    # delete stale removed skfs
                    await db.execute(rm_sql)
                    await db.commit()
            return skfs_to_remove
        except aiosqlite.Error as e:
            print('[SQL Error: get_stale_skfs]\n', e)
            await db.rollback()


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
