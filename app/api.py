import os
import json
import asyncio
import grpc
import redis.asyncio as redis
from google.protobuf.json_format import MessageToDict
from chirpstack_api import api
from dotenv import load_dotenv


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# GLOBAL VARIABLES
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
load_dotenv()
CHIRPSTACK_HOST = os.getenv('CHIRPSTACK_SERVER')
CHIRPSTACK_APIKEY = os.getenv('CHIRPSTACK_APIKEY')
AUTH_TOKEN = [('authorization', f'Bearer {CHIRPSTACK_APIKEY}')]

# Single shared channel to ChirpStack - gRPC channels are meant to be
# long-lived and multiplex many concurrent RPCs, so one channel for the
# whole process is correct rather than opening/closing one per call.
_channel = grpc.aio.insecure_channel(CHIRPSTACK_HOST)

_redis = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=6379,
    db=0,
    decode_responses=True,
)
# Short-lived cache to dedupe the guaranteed double-fetch of every device at
# startup (first_sync_session_keys immediately followed by devices_sync_upsert's
# first loop). Kept well below the sync interval so periodic syncs still see
# fresh data; event-driven callers (join/update) bypass it with use_cache=False.
DEVICE_DATA_CACHE_TTL = 30  # seconds


async def close_channel():
    """Call this once when the script shuts down."""
    await _channel.close()


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
#  Get device EUI's
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
async def get_device_euis(dev_eui) -> int | int:
    client = api.DeviceServiceStub(_channel)
    req = api.GetDeviceRequest()
    req.dev_eui = dev_eui
    resp = await client.Get(req, metadata=AUTH_TOKEN)
    data = MessageToDict(resp)['device']
    return data['devEui'], data['joinEui']


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
#  Functions for database device sync
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
async def get_tenant_list() -> list[str]:
    client = api.TenantServiceStub(_channel)
    # # Define the API key meta-data.
    req = api.ListTenantsRequest()
    req.limit = 1000  # mandatory if you want details.
    resp = await client.List(req, metadata=AUTH_TOKEN)
    tenants = [x['id'] for x in MessageToDict(resp)['result']]
    return tenants


async def get_tennant_apps(tenant_id: str) -> list[str]:
    client = api.ApplicationServiceStub(_channel)
    # # Define the API key meta-data.
    req = api.ListApplicationsRequest()
    req.limit = 1000  # mandatory if you want details.
    req.tenant_id = tenant_id
    resp = await client.List(req, metadata=AUTH_TOKEN)
    data = MessageToDict(resp)
    if data.get('result'):
        return [x['id'] for x in data['result']]
    return


async def get_application_devices(application_id: str) -> list[str]:
    client = api.DeviceServiceStub(_channel)
    # # Construct request.
    req = api.ListDevicesRequest()
    req.limit = 1000  # mandatory if you want details.
    req.application_id = application_id
    resp = await client.List(req, metadata=AUTH_TOKEN)
    devices = MessageToDict(resp)
    if devices.get('result'):
        return [x['devEui'] for x in devices['result']]
    return


async def get_device_data(dev_eui: str, use_cache: bool = True) -> dict:
    """ example full output
    {
        "devEui": "2cf7f1c053800000",
        "name": "T1000A-WDRIoT-004",
        "description": "Disabled",
        "applicationId": "826ffd30-0286-43e9-b174-d58d3aabc1f0",
        "deviceProfileId": "abd8d5af-8d58-49ab-a420-a4ff028ba72b",
        "variables": {
            "ThingsBoardAccessToken": "PpG1jeVwwx6erVnSnF1c",
            "max_copies": "10",     // optional...
            "private": "false"      // optional...
        },
        "joinEui": "7c3b5e861683b000",
        "skipFcntCheck": false,
        "isDisabled": false,
        "tags": {
            "max_copies": "10",     // optional...
            "private": "false"      // optional...
        },
        "devAddr": "780001e6",
        "appSKey": "f618237213154bb1886b2b5370bf4000",
        "nwkSEncKey": "badfa2746a9aa9f022534f941d373000",
        "fCntUp": 467,
        "nFCntDown": 13,
        "sNwkSIntKey": "badfa2746a9aa9f022534f941d373000",
        "fNwkSIntKey": "badfa2746a9aa9f022534f941d373000",
        "aFCntDown": 0,
        "nwkKey": "dcf45e151d003f8b707afbb875f72000",
        "appKey": "00000000000000000000000000000000"
        }
    """
    cache_key = f'device_data:{dev_eui}'

    if use_cache:
        try:
            cached = await _redis.get(cache_key)
            if cached:
                return json.loads(cached)
        except redis.RedisError as e:
            print('[Redis Error: get_device_data read]', e)

    client = api.DeviceServiceStub(_channel)
    req = api.GetDeviceRequest()
    req.dev_eui = dev_eui
    a = MessageToDict(await client.Get(req, metadata=AUTH_TOKEN), True)['device']
    b = MessageToDict(await client.GetActivation(req, metadata=AUTH_TOKEN), True)
    if b.get('deviceActivation'):
        b = b['deviceActivation']
        c = MessageToDict(await client.GetKeys(req, metadata=AUTH_TOKEN), True)['deviceKeys']
        data = a | b | c
    else:
        data = a | b

    try:
        await _redis.set(cache_key, json.dumps(data), ex=DEVICE_DATA_CACHE_TTL)
    except redis.RedisError as e:
        print('[Redis Error: get_device_data write]', e)

    return data


async def all_tenant_apps() -> list[str]:
    tenants = await get_tenant_list()
    if not tenants:
        return []

    # Run all tenant fetches concurrently
    results = await asyncio.gather(
        *(get_tennant_apps(tenant) for tenant in tenants),
        return_exceptions=True
    )

    # Flatten and filter valid lists, ignoring tenants with no applications
    return [
        app for result in results
        if isinstance(result, list)
        for app in result
    ]


async def all_tenant_deveui() -> list[str]:
    app_ids = await all_tenant_apps()
    if not app_ids:
        return []

    # Run all device fetches concurrently
    results = await asyncio.gather(
        *(get_application_devices(app_id) for app_id in app_ids),
        return_exceptions=True
    )

    # Flatten and filter valid lists, ignoring applications with no devices
    return [
        dev_eui for result in results
        if isinstance(result, list)
        for dev_eui in result
    ]
