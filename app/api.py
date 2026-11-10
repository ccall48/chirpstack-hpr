import os
import json
import time
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
# Created lazily (not here at import time): grpc.aio.Channel binds to
# whichever event loop is running when it's constructed, and this module
# is imported before asyncio.run() starts the real loop - building it here
# attaches it to the wrong loop and every RPC fails with
# "attached to a different loop".
_channel = None

_redis = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=6379,
    db=0,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)
# Key-changing callers bypass this cache and serialize fresh reads through writes.
DEVICE_DATA_CACHE_TTL = 30  # seconds
RPC_TIMEOUT = 10


async def _get_channel() -> grpc.aio.Channel:
    global _channel
    if _channel is None:
        _channel = grpc.aio.insecure_channel(CHIRPSTACK_HOST)
    return _channel


async def close_channel():
    """Call this once when the script shuts down."""
    if _channel is not None:
        await _channel.close()


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
#  Get device EUI's
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
async def get_device_euis(dev_eui) -> int | int:
    client = api.DeviceServiceStub(await _get_channel())
    req = api.GetDeviceRequest()
    req.dev_eui = dev_eui
    resp = await client.Get(req, metadata=AUTH_TOKEN, timeout=RPC_TIMEOUT)
    data = MessageToDict(resp)['device']
    return data['devEui'], data['joinEui']


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
#  Functions for database device sync
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
PAGE_SIZE = 1000


async def _list_all(list_fn, build_request, get_id):
    """Page through a ChirpStack List RPC (limit/offset/total_count) until exhausted."""
    ids = []
    offset = 0
    while True:
        req = build_request(PAGE_SIZE, offset)
        resp = await list_fn(req, metadata=AUTH_TOKEN, timeout=RPC_TIMEOUT)
        ids.extend(get_id(item) for item in resp.result)
        offset += len(resp.result)
        if offset >= resp.total_count:
            return ids
        if not resp.result:
            raise RuntimeError('ChirpStack returned an incomplete listing')


async def get_tenant_list() -> list[str]:
    client = api.TenantServiceStub(await _get_channel())

    def build_request(limit, offset):
        req = api.ListTenantsRequest()
        req.limit = limit
        req.offset = offset
        return req

    return await _list_all(client.List, build_request, lambda item: item.id)


async def get_tennant_apps(tenant_id: str) -> list[str]:
    client = api.ApplicationServiceStub(await _get_channel())

    def build_request(limit, offset):
        req = api.ListApplicationsRequest()
        req.limit = limit
        req.offset = offset
        req.tenant_id = tenant_id
        return req

    return await _list_all(client.List, build_request, lambda item: item.id)


async def get_application_devices(application_id: str) -> list[str]:
    client = api.DeviceServiceStub(await _get_channel())

    def build_request(limit, offset):
        req = api.ListDevicesRequest()
        req.limit = limit
        req.offset = offset
        req.application_id = application_id
        return req

    return await _list_all(client.List, build_request, lambda item: item.dev_eui)


async def get_device_data(dev_eui: str, use_cache: bool = True) -> tuple[dict, float]:
    # The timestamp travels with cache entries for diagnostics. It is not a
    # ChirpStack revision; key-changing callers serialize uncached reads themselves.
    cache_key = f'device_data:{dev_eui}'

    if use_cache:
        try:
            cached = await _redis.get(cache_key)
            if cached:
                envelope = json.loads(cached)
                if isinstance(envelope, dict) and 'data' in envelope and 'observed_at' in envelope:
                    return envelope['data'], envelope['observed_at']
                # Ignore the old cache format during rolling upgrades.
        except redis.RedisError as e:
            print('[Redis Error: get_device_data read]', e)

    client = api.DeviceServiceStub(await _get_channel())
    req = api.GetDeviceRequest()
    req.dev_eui = dev_eui
    observed_at = time.time()
    a = MessageToDict(await client.Get(req, metadata=AUTH_TOKEN, timeout=RPC_TIMEOUT), True)['device']
    try:
        b = MessageToDict(await client.GetActivation(req, metadata=AUTH_TOKEN, timeout=RPC_TIMEOUT), True)
    except grpc.aio.AioRpcError as err:
        if err.code() != grpc.StatusCode.NOT_FOUND:
            raise
        b = {}
    if b.get('deviceActivation'):
        b = b['deviceActivation']
        try:
            c = MessageToDict(await client.GetKeys(req, metadata=AUTH_TOKEN, timeout=RPC_TIMEOUT), True)['deviceKeys']
        except grpc.aio.AioRpcError as err:
            if err.code() != grpc.StatusCode.NOT_FOUND:
                raise
            c = {}  # ABP devices can have an activation without root keys.
        data = a | b | c
    else:
        data = a | b

    try:
        await _redis.set(cache_key, json.dumps({'data': data, 'observed_at': observed_at}), ex=DEVICE_DATA_CACHE_TTL)
    except redis.RedisError as e:
        print('[Redis Error: get_device_data write]', e)

    return data, observed_at


async def all_tenant_apps() -> list[str]:
    tenants = await get_tenant_list()
    if not tenants:
        return []

    results = await asyncio.gather(
        *(get_tennant_apps(tenant) for tenant in tenants),
        return_exceptions=True
    )

    apps = []
    for tenant, result in zip(tenants, results):
        if isinstance(result, Exception):
            raise RuntimeError(f'failed to list applications for tenant {tenant}') from result
        if result:
            apps.extend(result)
    return apps


async def all_tenant_deveui() -> list[str]:
    app_ids = await all_tenant_apps()
    if not app_ids:
        return []

    results = await asyncio.gather(
        *(get_application_devices(app_id) for app_id in app_ids),
        return_exceptions=True
    )

    devices = []
    for app_id, result in zip(app_ids, results):
        if isinstance(result, Exception):
            raise RuntimeError(f'failed to list devices for application {app_id}') from result
        if result:
            devices.extend(result)
    return devices
