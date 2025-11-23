import redis.asyncio as redis
"""
intersection between device update and helium cli for updates?
"""


class DeviceRedis:
    def __init__(self):
        self.r_hpr_pool = redis.ConnectionPool(host='redis-hpr-v2', port=6379, db=0)
        self.r_hpr_db = redis.Redis(connection_pool=self.r_hpr_pool, decode_responses=True)

    async def device_update_stream(self, kwargs):
        try:
            await self.r_hpr_db.xadd('device:stream', kwargs, maxlen=100_000)
        except redis.RedisError as e:
            print('[RHPR device Error]', e)

    async def helium_cli_stream(self, kwargs):
        try:
            await self.r_hpr_db.xadd('skfs:stream', kwargs, maxlen=100_000)
        except redis.RedisError as e:
            print('[RHPR device Error]', e)

    async def tenant_dc_stream(self, kwargs):
        try:
            await self.r_hpr_db.xadd('tenant:stream', kwargs, maxlen=100_000)
        except redis.RedisError as e:
            print('[RHPR device Error]', e)

