import os
import asyncio
import time
import redis.asyncio as redis
import grpc
from google.protobuf.json_format import MessageToJson, MessageToDict
from chirpstack_api import api, integration, stream
from dotenv import load_dotenv

from models import DeviceDatabase
from schemas import GetDeviceSyncRequest

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


while True:
    asyncio.run(main())
