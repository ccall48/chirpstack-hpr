import redis.asyncio as redis
import json
"""
intersection between device update and helium cli for updates?
"""


class DeviceRedis:
    def __init__(self):
        # Let the client create and manage its own internal pool.
        # Set max_connections to match your expected peak concurrency.
        # Default is often unlimited (or very high), which can exhaust OS file descriptors.
        self.r = redis.Redis(
            host='redis-hpr-v2',
            port=6379,
            db=0,
            decode_responses=True,
            max_connections=100,  # Tune this: ~1 connection per concurrent task peak
            socket_timeout=5.0,   # Prevent hanging on slow network
            socket_connect_timeout=5.0
        )

    async def close(self):
        """Call this once when the script shuts down."""
        await self.r.aclose()

    async def device_update_stream(self, kwargs):
        try:
            await self.r.xadd('device:stream', kwargs, maxlen=100_000)
        except redis.RedisError as e:
            print('[RHPR device Error]', e)

    async def helium_cli_stream(self, kwargs):
        try:
            await self.r.xadd('skfs:stream', kwargs, maxlen=100_000)
        except redis.RedisError as e:
            print('[RHPR helium_cli_stream: device Error]', e)

    async def tenant_dc_stream(self, kwargs):
        try:
            # await self.r.xadd('tenant:stream', kwargs, maxlen=100_000)
            await self.r.xadd('tenant:stream', {'uplink': json.dumps(kwargs)}, maxlen=100_000)
        except redis.RedisError as e:
            print('[RHPR tenant_dc_stream: tenant Error]', e)
