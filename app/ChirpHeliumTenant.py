import os
import asyncio
import redis.asyncio as redis
from math import ceil
from google.protobuf.json_format import MessageToDict
from chirpstack_api import stream
import logging
from UsagePublisher import publish_usage_event

# -----------------------------------------------------------------------------
# CHIRPSTACK REDIS CONNECTION
# -----------------------------------------------------------------------------
redis_server = os.getenv('REDIS_HOST')
rpool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=rpool, decode_responses=True)


class ChirpstackTenant:
    def __init__(
        self,
        route_id: str,
        pool,
        chirpstack_host: str,
        chirpstack_token: str,
    ):
        self.route_id = route_id
        self.pool = pool
        self.cs_gprc = chirpstack_host
        self.auth_token = [('authorization', f'Bearer {chirpstack_token}')]

    async def db_transaction(self, query):
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(query)

    async def db_fetch(self, query):
        async with self.pool.acquire() as con:
            async with con.transaction():
                return await con.fetch(query)

    async def stream_meta(self):
        stream_key = 'stream:meta'
        last_id = '0'
        while True:
            try:
                resp = await rdb.xread({stream_key: last_id}, count=1, block=0)

                for message in resp[0][1]:
                    last_id = message[0]

                    if b"up" in message[1]:
                        b = message[1][b"up"]
                        pl = stream.meta_pb2.UplinkMeta()
                        pl.ParseFromString(b)
                        data = MessageToDict(pl)
                        await self.meta_up(data)

            except Exception as exc:
                logging.info(f'stream_meta: {exc}')

    async def meta_up(self, data: dict):
        dev_eui = data['devEui']
        dupes = len(data['rxInfo'])
        # dc = ceil(data['phyPayloadByteCount'] / 24)
        if 'applicationPayloadByteCount' not in data.keys():
            # if an empty msg is sent by a device it doesnt pass this key.
            print('********** applicationPayloadByteCount **********')
            print(data)
            dc = ceil(data['phyPayloadByteCount'] / 24)
            print('**********    **********  *********    **********')
        else:
            dc = ceil(data['applicationPayloadByteCount'] / 24)

        total_dc = dupes * dc
        logging.info(f"dev_eui: {dev_eui} | MSG DC {dc} | Dupes: {dupes} | Total DC: {total_dc}")
        query = """
            UPDATE helium_devices SET dc_used = (dc_used + {}) WHERE dev_eui='{}';
        """.format(total_dc, dev_eui)
        await self.db_transaction(query)

        if os.getenv('PUBLISH_USAGE_EVENTS') == 'True':
            # First we get the tenant id for the device...
            query = """
                SELECT application.tenant_id, application.id FROM application
                JOIN device ON application.id = device.application_id
                WHERE device.dev_eui = decode('%s', 'hex');
            """ % dev_eui
            result = await self.db_fetch(query)
            tenant_id = None
            application_id = None
            for row in result:
                tenant_id = row['tenant_id']
                application_id = row['id']
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                publish_usage_event,
                dev_eui, tenant_id, application_id, total_dc,
            )
        return
