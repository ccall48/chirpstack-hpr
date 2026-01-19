import os
import asyncio
import time
import json
import random
import redis.asyncio as redis
from google.protobuf.json_format import MessageToDict, MessageToJson
from chirpstack_api import integration, stream
from dotenv import load_dotenv

from models import DeviceDatabase
from redis_models import DeviceRedis
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


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# GLOBAL VARIABLES
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
load_dotenv()

CHIRPSTACK_HOST = os.getenv('CHIRPSTACK_SERVER')
CHIRPSTACK_APIKEY = os.getenv('CHIRPSTACK_APIKEY')
AUTH_TOKEN = [('authorization', f'Bearer {CHIRPSTACK_APIKEY}')]

route_id = os.getenv('ROUTE_ID', None)

redis_server = os.getenv('REDIS_HOST')
rpool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=rpool, decode_responses=True)

database = DeviceDatabase()
deviceredis = DeviceRedis()
hpr = HeliumConfigCli()


def sleep_time(start, stop, step):
    return random.randrange(start, stop, step)


async def async_run_every(func: str, interval: int):
    name = str(func)
    while True:
        try:
            print(f'{time.ctime()} Executing: {name}, sleeping: {interval} seconds.')
            await func()
            await asyncio.sleep(interval)
        except Exception as err:
            print(f'{name} Error: {err}')
            pass


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
        await database.upsert_device(devices)
        sleeping = sleep_time(3550, 3600, 5)
        print(f'END RUNNING SQLITE DB SYNC SLEEPING: {sleeping} mins')
        await asyncio.sleep(sleeping)


async def first_sync_session_keys():
    """Run first on start to ensure sync of all existing device session keys with hpr"""
    print('START FIRST HELIUM SESSIONKEY SYNC')
    devices = []
    device_euis = await all_tenant_deveui()
    for dev_eui in device_euis:
        # use api to collect device data
        device = await get_device_data(dev_eui)
        d = GetDeviceSyncRequest(**device)
        is_private = d.variables.get('private', False)
        max_copies = d.variables.get('max_copies', 0)

        if not d.nwkSEncKey:
            # skip if device does not have a skfs or joined yet.
            continue

        if d.isDisabled or is_private:
            # remove skfs for a disabled or private device on inital sync.
            print('Disabled', d.devEui, d.name)
            devices.append(
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=d.devAddr,
                    session_key=d.nwkSEncKey,
                    action=iot_config.ActionV1(1),
                    max_copies=max_copies
                )
            )
        else:
            # Sync enabled and roaming device skfs.
            print('Enabled', d.devEui, d.name)
            devices.append(
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=d.devAddr,
                    session_key=d.nwkSEncKey,
                    action=iot_config.ActionV1(0),
                    max_copies=max_copies
                )
            )

    for group in hpr.chunker(devices, 100):
        # use chunker to limit update to max 100 per request
        await hpr.route_skfs(group)
    print('END FIRST HELIUM SESSIONKEY SYNC')


async def sync_session_keys():
    while True:
        print('START RUNNING SKFS PURGE')
        await hpr.remove_stale_skfs()
        sleeping = sleep_time(43150, 43200, 5)
        print(f'END RUNNING SKFS PURGE SLEEPING: {sleeping} mins')
        await asyncio.sleep(sleeping)


async def redis_events_streams():
    request_stream = 'api:stream:request'
    device_stream = 'device:stream:event'
    _id = '0'

    while True:
        try:
            resp = await rdb.xread(
                streams={
                    request_stream: _id,
                    device_stream: _id,
                },
                count=1,
                block=0
            )

            for message in resp[0][1]:
                _id = message[0]
                _grpc = message[1]

                if b'request' in message[1]:
                    msg = message[1][b'request']
                    if b'inform' in msg:
                        # ignore {"service": "inform"}
                        continue

                    pl = stream.ApiRequestLog()
                    pl.ParseFromString(msg)
                    req = MessageToDict(pl)

                    if 'method' not in req:
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

                    if d.variables.get('private', False):
                        # if device is private, do not sync with hpr.
                        sync_join_skfs = [
                            iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                                devaddr=d.devAddr,
                                session_key=d.nwkSEncKey,
                                # 0 add, 1 remove
                                action=iot_config.ActionV1(1),
                                # device max_copies if set, else 0 for default
                                max_copies=d.variables.get('max_copies', 1)
                            )
                        ]
                    else:
                        # sync with Helium Pakcet Router
                        sync_join_skfs = [
                            iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                                devaddr=d.devAddr,
                                session_key=d.nwkSEncKey,
                                # 0 add, 1 remove
                                action=iot_config.ActionV1(0),
                                # set max_copies or 0 for default route amount
                                max_copies=d.variables.get('max_copies', 0)
                            )
                        ]
                    await hpr.route_skfs(sync_join_skfs)

                if b'up' in message[1]:
                    msg = message[1][b'up']
                    pl = integration.UplinkEvent()
                    pl.ParseFromString(msg)
                    req = MessageToDict(pl, always_print_fields_with_no_presence=True)

                    tenant_id = req['deviceInfo']['tenantId']
                    tenant_name = req['deviceInfo']['tenantName']
                    device_name = req['deviceInfo']['deviceName']
                    # device_eui = req['deviceInfo']['devEui']

                    # avoid creating a list, only iterate over data once
                    hotspots = sum(1 for gw in req['rxInfo'] if gw.get('metadata', {}).get('network') == 'helium_iot')

                    if req.get('data'):
                        print('Data:', req['data'])

                        dc = data_bytes_size(req['data'])
                        total_dc = dc * hotspots

                        print('Tenant:', tenant_name, 'Device:', device_name)
                        print('Hotspot Count:', hotspots, 'DC Used:', total_dc)
                        # decouple sqlite tenant dc count to redis stream?
                        await database.upsert_data_credits(tenant_id, tenant_name, total_dc)
                        #
                        await deviceredis.tenant_dc_stream({
                            'tenant_id': tenant_id,
                            'tenant_name': tenant_name,
                            'device_name': device_name,
                            # 'device_eui': device_eui,
                            'dc_used': total_dc,
                        })
                    else:
                        # blank uplink data cost 1 DC * hotspots seen
                        print('Tenant:', tenant_name, 'Device:', device_name)
                        print('Hotspot Count:', hotspots, 'DC Used:', hotspots)
                        # decouple sqlite tenant dc count to redis stream?
                        await database.upsert_data_credits(tenant_id, tenant_name, hotspots)
                        #
                        await deviceredis.tenant_dc_stream({
                            'tenant_id': tenant_id,
                            'tenant_name': tenant_name,
                            'device_name': device_name,
                            # 'device_eui': device_eui,
                            'dc_used': hotspots,
                        })

                    print('^ ^ ^ ^ ^ ^ ^ DEVICE UPLINK EVENT ^ ^ ^ ^ ^ ^ ^')

            await asyncio.sleep(0)

        except Exception as exc:
            print('* * * * * * * * * v ERROR v * * * * * * * * *')
            print(f'[Error]\n: {exc}')
            print('[GRPC]\n', _grpc)
            print('* * * * * * * * * ^ ERROR ^ * * * * * * * * *')
            pass


async def main():
    # create sqlite db and tables if not exists
    await database.create_tables()
    # handle initial sync of skfs for devices on start
    await first_sync_session_keys()
    # short sleep before init async tasks.
    await asyncio.sleep(2)

    tasks = [
        redis_events_streams(),
        devices_sync_upsert(),
        get_helium_skfs(),
        sync_session_keys(),
        # async_run_every(first_sync_session_keys, 600)
    ]
    await asyncio.gather(*tasks)

asyncio.run(main())
