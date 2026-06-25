import os
import asyncio
from itertools import chain
import grpc
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


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
#  Get device EUI's
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
async def get_device_euis(dev_eui) -> int | int:
    async with grpc.aio.insecure_channel(CHIRPSTACK_HOST) as channel:
        client = api.DeviceServiceStub(channel)
        req = api.GetDeviceRequest()
        req.dev_eui = dev_eui
        resp = await client.Get(req, metadata=AUTH_TOKEN)
        data = MessageToDict(resp)['device']
    return data['devEui'], data['joinEui']


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
#  Functions for database device sync
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
async def get_tenant_list() -> list[str]:
    async with grpc.aio.insecure_channel(CHIRPSTACK_HOST) as channel:
        client = api.TenantServiceStub(channel)
        # # Define the API key meta-data.
        req = api.ListTenantsRequest()
        req.limit = 1000  # mandatory if you want details.
        resp = await client.List(req, metadata=AUTH_TOKEN)
        tenants = (x['id'] for x in MessageToDict(resp)['result'])
    return tenants


async def get_tennant_apps(tenant_id: str) -> list[str]:
    async with grpc.aio.insecure_channel(CHIRPSTACK_HOST) as channel:
        client = api.ApplicationServiceStub(channel)
        # # Define the API key meta-data.
        req = api.ListApplicationsRequest()
        req.limit = 1000  # mandatory if you want details.
        req.tenant_id = tenant_id
        resp = await client.List(req, metadata=AUTH_TOKEN)
        data = MessageToDict(resp)
        if data.get('result'):
            apps = (x['id'] for x in data['result'])
            return apps
    return


async def get_application_devices(application_id: str) -> list[str]:
    async with grpc.aio.insecure_channel(CHIRPSTACK_HOST) as channel:
        client = api.DeviceServiceStub(channel)
        # # Construct request.
        req = api.ListDevicesRequest()
        req.limit = 1000  # mandatory if you want details.
        req.application_id = application_id
        resp = await client.List(req, metadata=AUTH_TOKEN)
        devices = MessageToDict(resp)
        if devices.get('result'):
            apps = (x['devEui'] for x in devices['result'])
            return apps
    return


async def get_device_data(dev_eui: str) -> dict:
    """ example full output
    {
        "devEui": "2cf7f1c053800000",
        "name": "T1000A-WDRIoT-004",
        "description": "Disabled",
        "applicationId": "826ffd30-0286-43e9-b174-d58d3aabc1f0",
        "deviceProfileId": "abd8d5af-8d58-49ab-a420-a4ff028ba72b",
        "variables": {
            "ThingsBoardAccessToken": "PpG1jeVwwx6erVnSnF1c",
            "max_copies": "10"
        },
        "joinEui": "7c3b5e861683b000",
        "skipFcntCheck": false,
        "isDisabled": false,
        "tags": {},
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
    async with grpc.aio.insecure_channel(CHIRPSTACK_HOST) as channel:
        client = api.DeviceServiceStub(channel)
        req = api.GetDeviceRequest()
        req.dev_eui = dev_eui
        a = MessageToDict(await client.Get(req, metadata=AUTH_TOKEN), True)['device']
        b = MessageToDict(await client.GetActivation(req, metadata=AUTH_TOKEN), True)
        if b.get('deviceActivation'):
            b = b['deviceActivation']
            c = MessageToDict(await client.GetKeys(req, metadata=AUTH_TOKEN), True)['deviceKeys']
            return a | b | c
    return a | b


"""
async def all_tenant_apps() -> list[str]:
    all_apps = []
    for app in await get_tenant_list():
        apps = await get_tennant_apps(app)
        if apps is None:
            # ignore tenants with no applications that return None
            continue
        all_apps += (x for x in apps)
    return all_apps
"""

"""
async def all_tenant_apps() -> list[str]:
    # Get all tenants first
    tenants = await get_tenant_list()

    # Create tasks for all tenants to run concurrently
    tasks = [get_tennant_apps(app) for app in tenants]

    # Gather all results concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Flatten the list of lists, filtering out None and exceptions
    all_apps = [
        app for result in results
        if isinstance(result, list) and result is not None
        for app in result
    ]
    return all_apps
"""


async def all_tenant_apps() -> list[str]:
    tenants = await get_tenant_list()
    if not tenants:
        return []

    # Run all tenant fetches concurrently
    results = await asyncio.gather(
        *(get_tennant_apps(app) for app in tenants),
        return_exceptions=True
    )

    # Flatten and filter valid lists
    return [
        app for result in results
        if isinstance(result, list)
        for app in result
    ]


"""
async def all_tenant_deveui() -> list[str]:
    all_dev_euis = []
    app_id = await all_tenant_apps()
    for app_devices in app_id:
        devices = await get_application_devices(app_devices)
        if devices is None:
            # ignore applications with no devices that return None
            continue
        all_dev_euis += devices
    return all_dev_euis
"""
"""
async def all_tenant_deveui() -> list[str]:
    # Get all app IDs concurrently (using the optimized function above)
    app_ids = await all_tenant_apps()

    # Create tasks for all applications to run concurrently
    tasks = [get_application_devices(app_id) for app_id in app_ids]

    # Gather all results concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Flatten the list of lists, filtering out None and exceptions
    all_dev_euis = [
        dev_eui for result in results
        if isinstance(result, list) and result is not None
        for dev_eui in result
    ]
    return all_dev_euis
"""


async def all_tenant_deveui() -> list[str]:
    app_ids = await all_tenant_apps()
    if not app_ids:
        return []

    # Run all device fetches concurrently
    results = await asyncio.gather(
        *(get_application_devices(app_id) for app_id in app_ids),
        return_exceptions=True
    )

    # Flatten and filter valid lists
    return [
        dev_eui for result in results
        if isinstance(result, list)
        for dev_eui in result
    ]
