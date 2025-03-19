import os
import asyncio
# import time
import json
import random
import redis.asyncio as redis
# import grpc
from google.protobuf.json_format import MessageToDict, MessageToJson
from chirpstack_api import integration, stream
from dotenv import load_dotenv

from models import DeviceDatabase
from protos.helium import iot_config
from HeliumProtos import HeliumConfigCli
from schemas import GetDeviceSyncRequest
from helium_func import (
    data_bytes_size,
)
from api import (
    all_tenant_deveui,
    get_device_data,
)

route_id = os.getenv('ROUTE_ID', None)

# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# GLOBAL VARIABLES
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
load_dotenv()

CHIRPSTACK_HOST = os.getenv('CHIRPSTACK_SERVER')
CHIRPSTACK_APIKEY = os.getenv('CHIRPSTACK_APIKEY')
AUTH_TOKEN = [('authorization', f'Bearer {CHIRPSTACK_APIKEY}')]

redis_server = os.getenv('REDIS_HOST')
rpool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=rpool, decode_responses=True)

database = DeviceDatabase()
hpr = HeliumConfigCli()


def sleep_time(start, stop, step):
    return random.randrange(start, stop, step)

# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# RUN PROGRAM
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
async def get_helium_skfs():
    while True:
        print('START HELIUM SKFS')
        skfs = await hpr.route_skfs_list()
        sleeping = sleep_time(3550, 3600, 5)
        print(f'END HELIUM SKFS, SLEEPING: {sleeping} mins')
        # update synced helium skfs
        await database.upsert_helium_skfs(skfs)
        # await asyncio.sleep(600)
        await asyncio.sleep(sleeping)


async def devices_sync_upsert():
    while True:
        print('START RUNNING SQLITE DB SYNC')
        devices = []
        device_euis = await all_tenant_deveui()
        for dev_eui in device_euis:
            # # use api to collect device data
            device = await get_device_data(dev_eui)
            d = GetDeviceSyncRequest(**device)
            devices.append((
                str(d.devEui),
                str(d.name),
                str(d.isDisabled),
                json.dumps(d.variables),
                str(d.tags),
                str(d.joinEui),
                str(d.devAddr),
                str(d.nwkKey),
                str(d.appSKey),
                str(d.nwkSEncKey),
                route_id,
            ))
        # print(devices)
        await database.upsert_device(devices)
        sleeping = sleep_time(3550, 3600, 5)
        print(f'END RUNNING SQLITE DB SYNC SLEEPING: {sleeping} mins')
        # sleep 5 minutes / 300 seconds...
        # await asyncio.sleep(300)
        await asyncio.sleep(sleeping)


async def sync_session_keys():
    while True:
        print('START RUNNING SKFS PURGE')
        await hpr.remove_stale_skfs()
#        skfs_to_remove = await database.get_stale_skfs()
#        # add chunker for if > 100 devices
#        print(skfs_to_remove)
#        await hpr.route_skfs(skfs_to_remove)
        sleeping = sleep_time(43150, 43200, 5)
        print(f'END RUNNING SKFS PURGE SLEEPING: {sleeping} mins')
        # await asyncio.sleep(600)
        await asyncio.sleep(sleeping)


async def redis_events_streams():
    # create sqlite db tables if not exist...
    await database.create_tables()

    # i = 0
    request_stream = 'api:stream:request'
    device_stream = 'device:stream:event'
    lid_id = '0'

    while True:
        try:
            resp = await rdb.xread(
                streams={
                    request_stream: lid_id,
                    device_stream: lid_id,
                },
                count=1,
                block=0
            )

            for message in resp[0][1]:
                lid_id = message[0]
                _grpc = message[1]

                if b'request' in message[1]:
                    msg = message[1][b'request']
                    if b'inform' in msg:
                        # ignore {"service": "inform"}
                        continue

                    # print(f'{i} = v REQUEST v EVENT REQUEST v REQUEST v =')
                    # print(msg)

                    pl = stream.ApiRequestLog()
                    pl.ParseFromString(msg)
                    req = MessageToDict(pl)

                    if 'method' not in req.keys():
                        continue

                    match req['service']:
                        case 'api.DeviceService':
                            if req['method'] == 'Create':
                                print('========== API Create Euis ==========')
                                print(MessageToJson(pl))
                                await hpr.add_device_euis(req['metadata'])

                            if req['method'] == 'Delete':
                                print('========== API Delete Euis ==========')
                                print(MessageToJson(pl))
                                await hpr.remove_device_euis(req['metadata'])

                            if req['method'] == 'Update':
                                print('========== API Update Euis ==========')
                                print(MessageToJson(pl))
                                await hpr.update_device(req['metadata'])
                    # print('- - - - - - - - - - - - - - - - - - - - - - - - -')

                if b'join' in message[1]:
                    msg = message[1][b'join']
                    print(msg)
                    pl = integration.JoinEvent()
                    pl.ParseFromString(msg)
                    dev_eui = MessageToDict(pl)["deviceInfo"]["devEui"]
                    print(f'JOIN DEV_EUI: {dev_eui}')
                    print('- - - - - -')
                    activate = await get_device_data(dev_eui)
                    print(activate)
                    d = GetDeviceSyncRequest(**activate)
                    device = [(
                        str(d.devEui),
                        str(d.name),
                        str(d.isDisabled),
                        json.dumps(d.variables),
                        str(d.tags),
                        str(d.joinEui),
                        str(d.devAddr),
                        str(d.nwkKey),
                        str(d.appSKey),
                        str(d.nwkSEncKey),
                        route_id
                    )]
                    print('=====>')
                    print(device)
                    await database.upsert_device(device)

                    # sync with Helium Pakcet Router
                    add_join_skfs = [
                        iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                            devaddr=d.devAddr,
                            session_key=d.nwkSEncKey,
                            # 0 add, 1 remove
                            action=iot_config.ActionV1(0),
                            # device max_copies if set, else 0 for default
                            max_copies=d.variables.get('max_copies', 0)
                        )
                    ]
                    await hpr.route_skfs(add_join_skfs)
                # print('- - - - - - - - - - - - - - - - - - - - - - - - -')

                if b'up' in message[1]:
                    msg = message[1][b'up']
                    pl = integration.UplinkEvent()
                    pl.ParseFromString(msg)
                    req = MessageToDict(pl)

                    # # print uplink information
                    # print(json.dumps(req, indent=4))

                    tenant_id = req['deviceInfo']['tenantId']
                    tenant_name = req['deviceInfo']['tenantName']

                    # avoid creating an intermediate list, only iterate over data once
                    hotspots = sum(1 for gw in req['rxInfo'] if gw.get('metadata', {}).get('network') == 'helium_iot')

                    if req.get('data'):
                        print('Data:', req['data'])

                        dc = data_bytes_size(req['data'])
                        total_dc = dc * hotspots
                        print('Hotspot Count:', hotspots, 'DC Used:', total_dc)
                        await database.upsert_data_credits(tenant_id, tenant_name, total_dc)
                    else:
                        # blank uplink data cost * hotspots seen
                        print('Hotspot Count:', hotspots, 'DC Used:', hotspots)
                        await database.upsert_data_credits(tenant_id, tenant_name, hotspots)

                    print('^ ^ ^ ^ ^ ^ ^ DEVICE UPLINK EVENT ^ ^ ^ ^ ^ ^ ^')
                # i += 1
            await asyncio.sleep(0)

        except Exception as exc:
            print('* * * * * * * * * v ERROR v * * * * * * * * *')
            print(f'[Error]\n: {exc}')
            print('[GRPC]\n', _grpc)
            print('* * * * * * * * * ^ ERROR ^ * * * * * * * * *')
            pass


async def main():
    tasks = [
        redis_events_streams(),
        devices_sync_upsert(),
        get_helium_skfs(),
        sync_session_keys(),
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
