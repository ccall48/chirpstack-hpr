import os
import asyncio
import ujson
import grpc
import redis.asyncio as redis
from google.protobuf.json_format import MessageToJson
from chirpstack_api import api


# -----------------------------------------------------------------------------
# CHIRPSTACK GRPC CONNECTION
# -----------------------------------------------------------------------------
cs_server = os.getenv('CHIRPSTACK_SERVER')
cs_api_key = os.getenv('CS_APIKEY')
auth_token = [('authorization', f'Bearer {cs_api_key}')]

# -----------------------------------------------------------------------------
# CHIRPSTACK REDIS CONNECTION
# -----------------------------------------------------------------------------
redis_server = os.getenv('REDIS_HOST')
pool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=pool, decode_responses=True)


async def gateway_frames():
    stream_key = 'gw:stream:frame'
    last_id = '0'
    try:
        while True:
            resp = await rdb.xread({stream_key: last_id}, count=1, block=0)

            for message in resp[0][1]:
                last_id = message[0].decode()

                if b"up" in message[1]:
                    b = message[1][b"up"]
                    pl = api.frame_log_pb2.UplinkFrameLog()
                    pl.ParseFromString(b)
                    print("====== [ UPLINK Gateway message... ] ======")
                    print(MessageToJson(pl))
                    # client.publish("gateway/frame/up", MessageToJson(pl))

                if b"down" in message[1]:
                    b  = message[1][b"down"]
                    pl = api.frame_log_pb2.DownlinkFrameLog()
                    pl.ParseFromString(b)
                    print("====== [ DOWNLINK Gateway message... ] ======")
                    print(MessageToJson(pl))
                    # client.publish("gateway/frame/down", MessageToJson(pl))

    except Exception as exc:
        print(f'Error: {exc}')
        # log exception error here when adding logger
        pass


asyncio.run(gateway_frames())
