import os
import asyncio
import json
import grpc
from google.protobuf.json_format import MessageToJson, MessageToDict
from chirpstack_api import api, integration, stream
from dotenv import load_dotenv


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# GLOBAL VARIABLES
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
load_dotenv()
CHIRPSTACK_HOST = os.getenv('CHIRPSTACK_SERVER')
CHIRPSTACK_APIKEY = os.getenv('CHIRPSTACK_APIKEY')
AUTH_TOKEN = [('authorization', f'Bearer {CHIRPSTACK_APIKEY}')]


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
    async with grpc.aio.insecure_channel(CHIRPSTACK_HOST) as channel:
        client = api.DeviceServiceStub(channel)
        req = api.GetDeviceRequest()
        req.dev_eui = dev_eui
        a = MessageToDict(await client.Get(req, metadata=AUTH_TOKEN), True)['device']
        b = MessageToDict(await client.GetActivation(req, metadata=AUTH_TOKEN), True)
        if b.get('deviceActivation'):
            b = b['deviceActivation']
    return a | b


async def all_tenant_apps() -> list[str]:
    all_apps = []
    for app in await get_tenant_list():
        apps = await get_tennant_apps(app)
        if apps is None:
            # ignore tenants with no applications that return None
            continue
        all_apps += (x for x in apps)
    return all_apps


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
