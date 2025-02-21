import aiosqlite
from schemas import GetDeviceSyncRequest


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
                    nwkSEncKey TEXT
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

    async def upsert_device(self, kwargs):
        # device = GetDeviceSyncRequest(**kwargs)
        async with aiosqlite.connect(self.database) as db:
            try:
                sql = """
                    INSERT INTO devices
                    (devEui, name, isDisabled, variables, tags, joinEui, devAddr, nwkKey, appSKey, nwkSEncKey)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(devEui) DO UPDATE
                    SET name=EXCLUDED.name,
                        isDisabled=EXCLUDED.isDisabled,
                        variables=EXCLUDED.variables,
                        tags=EXCLUDED.tags,
                        joinEui=EXCLUDED.joinEui,
                        devAddr=EXCLUDED.devAddr,
                        nwkKey=EXCLUDED.nwkKey,
                        appSKey=EXCLUDED.appSKey,
                        nwkSEncKey=EXCLUDED.nwkSEncKey
                """
                await db.executemany(sql, kwargs)
                await db.commit()
            except aiosqlite.Error as e:
                print(e)
                await db.rollback()

    async def upsert_data_credits(self, tenantId, tenantName, dc_used):
        async with aiosqlite.connect(self.database) as db:
            try:
                sql = """
                    INSERT INTO data_credits
                    (tenantId, tenantName, dc_used)
                    VALUES (?, ?, ?)
                    ON CONFLICT(tenantId) DO UPDATE
                    SET tenantName=EXCLUDED.tenantName,
                        dc_used = dc_used + EXCLUDED.dc_used
                """
                await db.execute(sql, (
                        tenantId,
                        tenantName,
                        dc_used,
                    ))
                await db.commit()
            except Exception as e:
                print(e)
                await db.rollback()

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
