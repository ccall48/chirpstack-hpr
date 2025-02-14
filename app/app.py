import os
import asyncio
import time
import json
import redis.asyncio as redis
import grpc
from google.protobuf.json_format import MessageToJson, MessageToDict
from chirpstack_api import api, integration, stream
from dotenv import load_dotenv

from models import DeviceDatabase
from schemas import GetDeviceSyncRequest
from helper import data_bytes_size
from api import (
    all_tenant_apps,
    all_tenant_deveui,
    get_device_data
)


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# GLOBAL VARIABLES
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
load_dotenv()

CHIRPSTACK_HOST = os.getenv('CHIRPSTACK_SERVER')
CHIRPSTACK_APIKEY = os.getenv('CHIRPSTACK_APIKEY')
AUTH_TOKEN = [('authorization', f'Bearer {CHIRPSTACK_APIKEY}')]

REDIS_SERVER = os.getenv('REDIS_HOST')
RPOOL = redis.ConnectionPool(host=REDIS_SERVER, port=6379, db=0)
RDB = redis.Redis(connection_pool=RPOOL, decode_responses=True)

database = DeviceDatabase()


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# RUN PROGRAM
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

async def main():
    await database.create_tables()

    device_euis = await all_tenant_deveui()
    for dev_eui in device_euis:
        device = await get_device_data(dev_eui)
        print(device)
        await database.upsert_device(device)
    await asyncio.sleep(20)

#while True:
#    asyncio.run(main())

async def redis_events_streams():
    await database.create_tables()
    i = 0
    request_stream = 'api:stream:request'
    device_stream = 'device:stream:event'
    lid_id = '0'

    while True:
        try:
            resp = await RDB.xread(
                streams={
                    request_stream: lid_id,
                    device_stream: lid_id,
                },
                count=1,
                block=0
            )

            for message in resp[0][1]:
                lid_id = message[0]
                grpc = message[1]

                if b'request' in message[1]:
                    msg = message[1][b'request']
                    if b'inform' in msg:
                        # ignore {"service": "inform"}
                        continue

                    print(f'{i} = REQUEST v REQUEST v EVENT REQUEST v REQUEST v REQUEST =')
                    print(msg)
                    pl = stream.api_request_pb2.ApiRequestLog()
                    pl.ParseFromString(msg)
                    req = MessageToDict(pl)
                    print(req)
                    print('- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -')

                if b'join' in message[1]:
                    msg = message[1][b'join']
                    print(msg)
                    pl = integration.integration_pb2.JoinEvent()
                    pl.ParseFromString(msg)
                    dev_eui = MessageToDict(pl)["deviceInfo"]["devEui"]
                    print(f'JOIN DEV_EUI: {dev_eui}')
                    print('- - - - - -')
                    activate = await get_device_data(dev_eui)
                    print(activate)
                    print('- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -')

                if b'up' in message[1]:
                    msg = message[1][b'up']
                    pl = integration.UplinkEvent()
                    pl.ParseFromString(msg)
                    req = MessageToDict(pl)

                    # print uplink information
                    print(json.dumps(req, indent=4))

                    # avoid creating an intermediate list and only iterate over the data once
                    hotspots = sum(
                        1 for gw in req['rxInfo']
                        if gw['metadata'].get('network') == 'helium_iot'
                    )

                    # device_data = await get_device_data(req['deviceInfo']['devEui'])
                    # print(device_data)

                    if req.get('data'):
                        print('Data:', req['data'])

                        dc = data_bytes_size(req['data'])
                        count = dc * hotspots
                        print('Hotspot Count:', hotspots, 'DC Used:', count)
                        await database.upsert_data_credits(req['deviceInfo']['tenantId'], req['deviceInfo']['tenantName'], count)
                    else:
                        # blank uplink data cost * hotspots seen
                        print('Hotspot Count:', hotspots, 'DC Used:', hotspots)
                        await database.upsert_data_credits(req['deviceInfo']['tenantId'], req['deviceInfo']['tenantName'], hotspots)

                    print('^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ DEVICE UPLINK EVENT ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^')

                i += 1

        except Exception as exc:
            print('********** ********** v ERROR v ********** **********')
            print(f'Error: {exc}')
            print('[GRPC]\n', grpc)
            print('********** ********** ^ ERROR ^ ********** **********')
            pass

asyncio.run(redis_events_streams())
