import os
import subprocess
import asyncio
import ujson
import grpc
import redis.asyncio as redis
from google.protobuf.json_format import MessageToJson
from chirpstack_api import api


# ENVIRONMENT VARIABLES
helium_config_host = os.getenv('HELIUM_CONFIG_HOST')
helium_keypair_bin = os.getenv('HELIUM_KEYPAIR_BIN')
helium_net_id = os.getenv('HELIUM_NET_ID')
helium_oui = os.getenv('HELIUM_OUI')
helium_max_copies = os.getenv('HELIUM_MAX_COPIES')
server_host = os.getenv('SERVER_HOST')
server_ip = os.getenv('SERVER_IP')
devaddr_start = os.getenv('DEVADDR_START')
devaddr_end = os.getenv('DEVADDR_END')
netid_start = os.getenv('NETID_START')
netid_end = os.getenv('NETID_END')
net_mask = os.getenv('NETID_MASK')
route_id = os.getenv('ROUTE_ID')

# CHIRPSTACK GRPC CONNECTION
cs_server = os.getenv('CHIRPSTACK_SERVER')
cs_api_key = os.getenv('CS_APIKEY')
auth_token = [('authorization', f'Bearer {cs_api_key}')]

# REDIS CHIRPSTACK CONNECTION
redis_server = os.getenv('REDIS_HOST')
pool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=pool, decode_responses=True)


# helper functions...
def helium_packet_router(cmd: str):
    p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE)
    out, err = p.communicate()
    return out


async def stream_requests():
    stream_key = "api:stream:request"
    last_id = '0'
    while True:
        try:
            resp = await rdb.xread({stream_key: last_id}, count=1, block=0)
            for message in resp[0][1]:
                last_id = message[0]

                if b'request' in message[1]:
                    msg = message[1][b'request']
                    # print(b)
                    pl = api.request_log_pb2.RequestLog()
                    pl.ParseFromString(msg)
                    # print(MessageToujson(pl))
                    req = ujson.loads(MessageToJson(pl))
                    if 'method' not in req.keys():
                        continue
                    #if 'device_profile_id' in req.keys():
                    #    continue

                    match req['method']:
                        case 'Create':
                            print('====== [Create Device Euis...] ======')
                            print(MessageToJson(pl))
                            await add_device_euis(req['metadata'])
                        case 'Delete':
                            print('====== [Delete Device Euis...] ======')
                            print(MessageToJson(pl))
                            await remove_device_euis(req['metadata'])
                        case 'Update':
                            print('====== [Update Device Euis...] ======')
                            print(MessageToJson(pl))
                            await update_device_euis(req['metadata'])


        except Exception as exc:
            print(f'Error: {exc}')
            pass


async def add_device_euis(data: dict):
    """
    add device:
      adds a device to hpr when added as a device within the chirpstack api or webui.
    """
    if 'dev_eui' not in data.keys():
        return
    device = data['dev_eui']
    dev_eui, join_eui = await route_handler(device)
    print(dev_eui, join_eui)
    # cmd = f'hpr route euis list --route-id {route_id}'
    cmd = f'hpr route euis add -d {dev_eui} -a {join_eui} --route-id {route_id} --commit'
    res = helium_packet_router(cmd)
    return print(res)


async def remove_device_euis(data: dict):
    """
    todo:
      save device and join euis to a lookup db, as they're deleted and are unable to be called
      on delete from chirpstack devices. for now device euis will need to be manually removed.
    """
    if 'dev_eui' not in data.keys():
        return
    device = data['dev_eui']
    print(device)
    print(ujson.dumps(data, indent=2))
    return True


async def update_device_euis(data: dict):
    """
    completes an action set on device to helium packet router.
      - disabling a device will remove it from route id.
      - enabling a device will add it back to the route id.
      - toggling on/off should allow for a previously unconnected device to be added to hpr.
    """
    if 'dev_eui' not in data.keys():
        return
    device = data['dev_eui']
    is_disabled = data['is_disabled']
    dev_eui, join_eui = await route_handler(device)
    # print('updating... ', dev_eui, join_eui)
    if is_disabled == 'true':
        # print('disabled, remove from route.. logic..')
        cmd = f'hpr route euis remove -d {dev_eui} -a {join_eui} --route-id {route_id} --commit'
    else:
        # print('enabled, add back to route.. logic..')
        cmd = f'hpr route euis add -d {dev_eui} -a {join_eui} --route-id {route_id} --commit'
    res = helium_packet_router(cmd)
    return print(res)


async def route_handler(dev_eui: str):
    async with grpc.aio.insecure_channel(cs_server) as channel:
        client = api.DeviceServiceStub(channel)
        req = api.GetDeviceRequest()
        req.dev_eui = dev_eui
        resp = await client.Get(req, metadata=auth_token)
        data = ujson.loads(MessageToJson(resp))['device']
    return data['devEui'], data['joinEui']


asyncio.run(stream_requests())
