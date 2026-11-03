import os
import asyncio
import time
import random
import redis.asyncio as redis
from google.protobuf.json_format import MessageToDict, MessageToJson
from chirpstack_api import integration, stream
from dotenv import load_dotenv

from models import DeviceDatabase, device_row
from redis_models import DeviceRedis
from HeliumProtos import HeliumConfigCli
from schemas import GetDeviceSyncRequest
from helium_func import uplink_dc_used
from usage_publisher import create_usage_publisher, publish_usage_event
from event_reader import EventReader
from api import all_tenant_deveui, get_device_data


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

SYNC_INTERVAL_MIN = int(os.getenv('SYNC_INTERVAL_MIN_SECONDS', 300))
SYNC_INTERVAL_MAX = int(os.getenv('SYNC_INTERVAL_MAX_SECONDS', 600))
DEVICE_FETCH_CONCURRENCY = 50
MAX_MESSAGE_ATTEMPTS = 3


class SyncState:
    def __init__(self):
        self.complete = False


def sleep_time(start, stop, step):
    return random.randrange(start, stop, step)


def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


async def fetch_devices(device_euis, use_cache=False):
    # Call under hpr.sync_lock when the results will update device keys.
    async def fetch(dev_eui):
        try:
            device, observed_at = await get_device_data(dev_eui, use_cache=use_cache)
            return dev_eui, device, observed_at
        except Exception as err:
            # The next reconciliation pass retries; do not sleep holding the sync lock.
            print(f'[device fetch failed]: {dev_eui}: {err}')
            return dev_eui, None, None

    results = []
    for batch in _chunks(device_euis, DEVICE_FETCH_CONCURRENCY):
        results.extend(await asyncio.gather(*(fetch(dev_eui) for dev_eui in batch)))

    return [(dev_eui, device, observed_at) for dev_eui, device, observed_at in results if device is not None]


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# RUN PROGRAM
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
async def get_helium_skfs(hpr):
    while True:
        try:
            print(f'{time.ctime()} START HELIUM SKFS')
            skfs = await hpr.route_skfs_list()
            # update synced helium skfs
            await hpr.database.upsert_helium_skfs(skfs)
            print(f'{time.ctime()} END HELIUM SKFS: synced {len(skfs)} skfs')
        except Exception as err:
            print(f'[get_helium_skfs error]: {err}')
        sleeping = sleep_time(SYNC_INTERVAL_MIN, SYNC_INTERVAL_MAX, 5)
        await asyncio.sleep(sleeping)


async def devices_sync_upsert(hpr, sync_state):
    while True:
        sync_state.complete = False
        complete = True
        try:
            device_euis = await all_tenant_deveui()
            synced = 0
            for batch in _chunks(device_euis, DEVICE_FETCH_CONCURRENCY):
                # Hold through fresh reads, SQLite commit and Helium acknowledgement.
                # Release between batches so queued joins/updates can run first.
                async with hpr.sync_lock:
                    fetched = await fetch_devices(batch, use_cache=False)
                    devices = []
                    for dev_eui, device, observed_at in fetched:
                        try:
                            d = GetDeviceSyncRequest(**device)
                            devices.append((d, device_row(d, observed_at, route_id)))
                        except Exception as err:
                            print(f'[invalid device record]: {dev_eui}: {err}')
                    if len(devices) != len(batch):
                        complete = False
                    await hpr.database.upsert_device([row for _, row in devices])
                    updates = [hpr.skf_update(d) for d, _ in devices if d.nwkSEncKey]
                    for group in _chunks(updates, 100):
                        await hpr.route_skfs(group)
                    synced += len(devices)
            # Only a fully discovered, committed and reconciled pass permits purge.
            sync_state.complete = complete
            print(f'{time.ctime()} DEVICE/SKF SYNC: {synced}/{len(device_euis)} devices')
        except Exception as err:
            print(f'[device/SKF sync error]: {err}')
        # Retry failed registrations indefinitely, including failures after startup.
        delay = sleep_time(SYNC_INTERVAL_MIN, SYNC_INTERVAL_MAX, 5) if sync_state.complete else 5
        await asyncio.sleep(delay)


async def sync_session_keys(hpr, sync_state):
    while True:
        try:
            removed = await hpr.remove_stale_skfs(sync_state)
            print(f'{time.ctime()} SKFS PURGE: removed {removed} stale skfs')
        except Exception as err:
            print(f'[sync_session_keys error]: {err}')
        await asyncio.sleep(sleep_time(SYNC_INTERVAL_MIN, SYNC_INTERVAL_MAX, 5))


async def handle_message(message, hpr, publisher=None):
    if b'request' in message[1]:
        msg = message[1][b'request']
        if b'inform' in msg:
            # ignore {"service": "inform"}
            return

        pl = stream.ApiRequestLog()
        pl.ParseFromString(msg)
        req = MessageToDict(pl)

        if 'method' not in req:
            return

        match req['service']:
            case 'api.DeviceService':
                # Protobuf JSON omits an empty metadata map.
                if req['method'] in ('Create', 'Delete', 'Update') and not req.get('metadata', {}).get('dev_eui'):
                    print(f'[Skipping {req["method"]} without dev_eui]: {MessageToJson(pl)}')
                    return

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
        pl = integration.JoinEvent()
        pl.ParseFromString(msg)
        dev_eui = MessageToDict(pl)["deviceInfo"]["devEui"]
        await hpr.sync_device(dev_eui)

    if b'up' in message[1]:
        msg = message[1][b'up']
        pl = integration.UplinkEvent()
        pl.ParseFromString(msg)
        req = MessageToDict(pl)

        if publisher is not None:
            try:
                await publish_usage_event(publisher, req, message[0], route_id)
            except Exception as exc:
                # best-effort: don't stall device/join sync behind a billing outage
                print(f'[Usage publish failed, dropping event]: {exc}')

        tenant_id = req['deviceInfo']['tenantId']
        # Protobuf JSON omits empty optional fields.
        tenant_name = req['deviceInfo'].get('tenantName', '')
        device_name = req['deviceInfo'].get('deviceName', '')
        device_eui = req['deviceInfo']['devEui']

        # avoid creating a list, only iterate over data once
        hotspots = sum(1 for gw in req.get('rxInfo', []) if gw.get('metadata', {}).get('network') == 'helium_iot')

        total_dc = uplink_dc_used(req)

        if req.get('data'):
            print('Data:', req['data'])
        print('Tenant:', tenant_name, 'Device:', device_name)
        print('Hotspot Count:', hotspots, 'DC Used:', total_dc)
        # decouple sqlite tenant dc count to redis stream?
        await database.upsert_data_credits(tenant_id, tenant_name, total_dc)
        #
        await deviceredis.tenant_dc_stream({
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'device_name': device_name,
            'device_eui': device_eui,
            'dc_used': total_dc,
        })

        print('^ ============ ^ DEVICE UPLINK EVENT ^ ============ ^')


async def redis_events_streams(reader, hpr, publisher=None):
    failures = {}
    while True:
        try:
            resp = await reader.read()

            for stream_name, message in resp:
                key = (stream_name, message[0])
                try:
                    await handle_message(message, hpr, publisher)
                except Exception as exc:
                    # The offset stays put, so the next read retries this message.
                    failures[key] = failures.get(key, 0) + 1
                    print(f'[Error]: {stream_name} {message[0]} attempt {failures[key]}:\n {exc}')
                    print('^ * * * * * * * * * * ^ ERROR ^ * * * * * * * * * * ^')
                    if failures[key] < MAX_MESSAGE_ATTEMPTS:
                        await asyncio.sleep(5)
                        continue
                    print(f'[Skipping poison message]: {stream_name} {message[0]}')
                failures.pop(key, None)
                await reader.acknowledge(stream_name, message[0])

            await asyncio.sleep(0)

        except Exception as exc:
            print(f'[Error]:\n {exc}')
            print('^ * * * * * * * * * * ^ ERROR ^ * * * * * * * * * * ^')
            await asyncio.sleep(5)


async def main():
    publisher = create_usage_publisher()
    hpr = HeliumConfigCli(database)
    reader = EventReader(
        rdb, ['api:stream:request', 'device:stream:event'],
        checkpoint_key=(f'hpr:{route_id}:usage:{os.getenv("PUBLISH_USAGE_EVENTS_PROVIDER")}:offsets'
                        if publisher else None),
        tail_streams=['device:stream:event'],
    )
    sync_state = SyncState()
    await reader.initialize()
    await database.create_tables()

    tasks = [
        redis_events_streams(reader, hpr, publisher),
        devices_sync_upsert(hpr, sync_state),
        get_helium_skfs(hpr),
        sync_session_keys(hpr, sync_state),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        if publisher is not None:
            await publisher.close()


if __name__ == '__main__':
    asyncio.run(main())
