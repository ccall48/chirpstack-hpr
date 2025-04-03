import aiosqlite
from schemas import GetDeviceSyncRequest, GetRouteSkfsList
from protos.helium import iot_config


class DeviceDatabase:
    def __init__(self):
        self.database = 'chirpstack-hpr.db'

    async def create_tables(self):
        async with aiosqlite.connect(self.database) as db:
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
                    dc_balance TEXT,
                    dc_used TEXT,
                    dc_multiplier INT default 3
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
                    maxCopies TEXT DEFAULT 0
            )""")

    async def upsert_device(self, kwargs):
        # device = GetDeviceSyncRequest(**kwargs)
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
        async with aiosqlite.connect(self.database) as db:
            try:
                await db.executemany(sql, kwargs)
                await db.commit()
            except aiosqlite.Error as e:
                print('[SQL ERROR: upsert_device]\n', e)
                await db.rollback()

    async def upsert_data_credits(self, tenantId, tenantName, dc_used):
        sql = """
            INSERT INTO data_credits
            (tenantId, tenantName, dc_used)
            VALUES (:tenantId, :tenantName, :dc_used)
            ON CONFLICT(tenantId) DO UPDATE
            SET tenantName=:tenantName,
                dc_used = dc_used + :dc_used
        """
        async with aiosqlite.connect(self.database) as db:
            try:
                await db.execute(sql, (
                        tenantId,
                        tenantName,
                        dc_used,
                    ))
                await db.commit()
            except aiosqlite.Error as e:
                print('[SQL ERROR: upsert_data_credits]\n', e)
                await db.rollback()

    async def upsert_helium_skfs(self, kwargs):
        sql = """
            INSERT INTO helium_skfs
            (routeId, devaddr, sessionKey, maxCopies)
            VALUES (:routeId, :devaddr, :sessionKey, :maxCopies)
            ON CONFLICT DO NOTHING
        """
        async with aiosqlite.connect(self.database) as db:
            try:
                await db.executemany(sql, kwargs)
                await db.commit()
            except aiosqlite.Error as e:
                print('[SQL ERROR: upsert_helium_skfs]\n', e)
                await db.rollback()

    async def get_device_euis(self, dev_eui):
        sql = f"""
            SELECT devEui, joinEui
            FROM devices
            WHERE devEui = '{int(dev_eui, 16)}';
        """
        async with aiosqlite.connect(self.database) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(sql) as cursor:
                row = await cursor.fetchone()
            return int(row['devEui']), int(row['joinEui'])

    # # # # # # # # # #
    # Purge old device session keys from helium packet router
    # # # # #
    async def get_stale_skfs(self):
        sql = """
            SELECT * FROM helium_skfs
            WHERE sessionKey NOT IN (SELECT nwkSEncKey FROM devices);
        """
        rm_sql = """
            DELETE FROM helium_skfs
            WHERE sessionKey NOT IN (SELECT nwkSEncKey FROM devices);
        """
        skfs_to_remove = []
        async with aiosqlite.connect(self.database) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(sql) as cursor:
                async for row in cursor:
                    skfs_to_remove.append(row)
                # delete stale removed skfs
                await db.execute(rm_sql)
                await db.commit()
        return skfs_to_remove

#    async def purge_stale_skfs(self):
#        sql = """
#            DELETE FROM helium_skfs
#            WHERE sessionKey NOT IN (SELECT nwkSEncKey FROM devices);
#        """
#        async with aiosqlite.connect(self.database) as db:
#            await db.execute(sql)
#            await db.commit()


"""
devEui=3240324265253275232
name='T1000A-iZincit'
isDisabled=False
variables={'max_copies': 100, 'private': False}
tags={}
joinEui=16469707286779846324
devAddr=2013266407
appSKey='aceef1dd3c10bde78dc2a4f966d990e2'
nwkSEncKey='e5e9c6b47880087d9b3a5f21b495031d'


CREATE TABLE IF NOT EXISTS devices (
    -- id INTEGER PRIMARY KEY AUTOINCREMENT,
    devEui INT PRIMARY KEY,         -- 3240324265253275232
    name TEXT,                      -- 'T1000A-iZincit'
    isDisabled NUMERIC NOT NULL,    -- False
    variables TEXT,                 -- {'max_copies': 100, 'private': False}
    tags TEXT,                      -- {}
    joinEui INT NOT NULL,           -- 16469707286779846324
    devAddr INT NOT NULL,           -- 2013266407
    appSKey TEXT,                   -- 'aceef1dd3c10bde78dc2a4f966d990e2'
    nwkSEncKey TEXT                 -- 'e5e9c6b47880087d9b3a5f21b495031d'
)
"""
