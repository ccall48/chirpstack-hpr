import os
import redis.asyncio as redis
import grpc
from google.protobuf.json_format import MessageToJson, MessageToDict
from chirpstack_api import api, integration
import logging

from protos.helium import iot_config
from ChirpHeliumCrypto import update_device_skfs


# -----------------------------------------------------------------------------
# CHIRPSTACK REDIS CONNECTION
# -----------------------------------------------------------------------------
redis_server = os.getenv('REDIS_HOST')
rpool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=rpool, decode_responses=True)


class ChirpstackJoins:
    def __init__(
        self,
        route_id: str,
        chirpstack_host: str,
        chirpstack_token: str,
    ):
        self.route_id = route_id
        self.cs_grpc = chirpstack_host
        self.auth_token = [('authorization', f'Bearer {chirpstack_token}')]


    ###########################################################################
    # follow internal redis stream gRPC for actionable changes
    ###########################################################################
    async def device_stream_event(self):
        stream_key = 'device:stream:event'
        last_id = '0'
        while True:
            try:
                resp = await rdb.xread({stream_key: last_id}, count=1, block=0)

                for message in resp[0][1]:
                    last_id = message[0]

                    if b'join' in message[1]:
                        msg = message[1][b'join']
                        pl = integration.integration_pb2.JoinEvent()
                        pl.ParseFromString(msg)
                        data = MessageToDict(pl)
                        print('========== v JOIN v ==========')
                        print(data)
                        print('========== ^ JOIN ^ ==========')
                        await self.add_session_key(data)
                        # print(MessageToJson(pl))

            except Exception as err:
                logging.info(f'api_stream_requests: {err}')
                pass

    async def get_device_activation(self, dev_eui: str):
        async with grpc.aio.insecure_channel(self.cs_grpc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceActivationRequest()
            req.dev_eui = dev_eui
            resp = await client.GetActivation(req, metadata=self.auth_token)
            data = MessageToDict(resp)['deviceActivation']
            logging.info('deviceActivation:', data)
            print('deviceActivation:', data)
        return data

    async def add_session_key(self, dev_eui):
        device = await self.get_device_activation(dev_eui)
        dev_addr = device['devAddr']
        nws_key = device['nwkSEncKey']
        add_skfs = [
            iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                devaddr=int(dev_addr, 16),
                session_key=nws_key,
                action=iot_config.ActionV1(0),
                max_copies=0
            )
        ]
        logging.info('Adding Join Skfs', add_skfs)
        print('Adding Join Skfs', add_skfs)
        await update_device_skfs(self.route_id, add_skfs)
