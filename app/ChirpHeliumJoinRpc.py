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
        pool,
        chirpstack_host: str,
        chirpstack_token: str,
    ):
        self.route_id = route_id
        self.pool = pool
        self.cs_grpc = chirpstack_host
        self.auth_token = [("authorization", f"Bearer {chirpstack_token}")]

    async def db_transaction(self, query: str):
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(query)

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
                        print('========== v DECODED EVENT JOIN DEVICE UP MESSAGE v ==========')
                        pl = integration.integration_pb2.JoinEvent()
                        pl.ParseFromString(msg)
                        dev_eui = MessageToDict(pl)["deviceInfo"]["devEui"]
                        print(dev_eui)
                        # add session key for joined device
                        await self.add_session_key(dev_eui)
                        # add device session details to db

                        print('========== ^ DECODED EVENT JOIN DEVICE UP MESSAGE ^ ==========')

            except Exception as err:
                logging.info(f'api_stream_requests: {err}')
                pass

    async def get_device(self, dev_eui: str) -> dict[str]:
        async with grpc.aio.insecure_channel(self.cs_grpc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = await client.Get(req, metadata=self.auth_token)
            data = MessageToDict(resp)["device"]
        return data

    async def get_device_activation(self, dev_eui: str):
        async with grpc.aio.insecure_channel(self.cs_grpc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceActivationRequest()
            req.dev_eui = dev_eui
            resp = await client.GetActivation(req, metadata=self.auth_token)
            data = MessageToDict(resp)['deviceActivation']
            print('*** deviceActivation ***\n', data)
        return data

    async def add_session_key(self, dev_eui):
        device = await self.get_device(dev_eui)
        device_act = await self.get_device_activation(dev_eui)

        dev_addr = device_act['devAddr']
        nws_key = device_act['nwkSEncKey']
        add_skfs = [
            iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                devaddr=int(dev_addr, 16),
                session_key=nws_key,
                action=iot_config.ActionV1(0),
                max_copies=0
            )
        ]
        print('*** Adding Join Skfs ***\n', add_skfs)
        await update_device_skfs(self.route_id, add_skfs)

        devices = {
            "devAddr": "",
            "appSKey": "",
            "nwkSEncKey": "",
            "name": "",
        }

        devices.update(device)
        devices.update(device_act)

        max_copies = 0
        if devices.get("variables") and "max_copies" in devices.get("variables"):
            max_copies = devices["variables"]["max_copies"]
        if "fCntUp" not in devices.keys():
            devices["fCntUp"] = 0
        if "nFCntDown" not in devices.keys():
            devices["nFCntDown"] = 0

        query = """
            INSERT INTO helium_devices
            (dev_eui, join_eui, dev_addr, max_copies, aps_key, nws_key, dev_name, fcnt_up, fcnt_down)
            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')
            ON CONFLICT (dev_eui) DO UPDATE
            SET join_eui = '{1}',
                dev_addr = '{2}',
                max_copies = '{3}',
                aps_key = '{4}',
                nws_key = '{5}',
                dev_name = '{6}',
                fcnt_up = '{7}',
                fcnt_down = '{8}';
        """.format(
            devices["devEui"],
            devices["joinEui"],
            devices["devAddr"],
            max_copies,
            devices["appSKey"],
            devices["nwkSEncKey"],
            devices["name"],
            devices["fCntUp"],
            devices["nFCntDown"],
        )
        await self.db_transaction(query)
