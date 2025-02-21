import os
import json
import time
# import grpc
# from google.protobuf.json_format import MessageToJson, MessageToDict

import nacl.bindings
from helium_py.crypto.keypair import Keypair
from helium_py.crypto.keypair import SodiumKeyPair

from protos.helium import iot_config
from grpclib.client import Channel


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# HELIUM gRPC API CALLS
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
class HeliumConfigCli:
    def __init__(self):
        self.helium_host = os.getenv('HELIUM_HOST', default='mainnet-config.helium.io')
        self.helium_port = int(os.getenv('HELIUM_PORT', default=6080))
        self.helium_oui = int(os.getenv('HELIUM_OUI', default=None))
        self.route_id = os.getenv('ROUTE_ID', None)
        # self.delegate_key = os.getenv('HELIUM_KEYPAIR_BIN', default=None)
        self.delegate_key = r'/app/delegate_key.bin'

        with open(self.delegate_key, 'rb') as f:
            skey = f.read()[1:]
            self.delegate_keypair = Keypair(
                SodiumKeyPair(
                    sk=skey,
                    pk=nacl.bindings.crypto_sign_ed25519_sk_to_pk(skey)
                )
            )

    async def route_euis(self, app_eui: str, dev_eui: str, action: bool):
        """ Example device euis update, pairs to be sent as integers
            euis_action: list ->
            [
                iot_config.RouteUpdateEuisReqV1(
                    action=iot_config.ActionV1(`enum`), # 0 add, 1 remove
                    eui_pair=iot_config.EuiPairV1(
                        route_id=route_id,
                        app_eui=app_eui,
                        dev_eui=dev_eui
                ),
                ...
            ]
        """
        # app_eui = int(app_eui, 16)
        # dev_eui = int(dev_eui, 16)
        async with Channel(self.helium_host, self.helium_port) as channel:
            service = iot_config.RouteStub(channel)
            req = iot_config.RouteUpdateEuisReqV1(
                action=iot_config.ActionV1(action),
                eui_pair=iot_config.EuiPairV1(
                    route_id=self.route_id,
                    app_eui=app_eui,
                    dev_eui=dev_eui
                ),
                timestamp=int(time.time()),
                signer=self.delegate_keypair.address.bin
            )
            req.signature = self.delegate_keypair.sign(req.SerializeToString())
            resp = await service.update_euis([req])
        print(json.dumps(resp.to_dict(), indent=2))
        return

    async def route_skfs(self, skfs_action: list):
        """ Example of device session key update.
            skfs_action: list ->
            [
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=`uint32`,
                    session_key=`str`,
                    action=iot_config.ActionV1(`enum`), # 0 add, 1 remove
                    max_copies=`uint32`
                ),
                ...
            ]
        """
        async with Channel(self.helium_host, self.helium_port) as channel:
            service = iot_config.RouteStub(channel)
            req = iot_config.RouteSkfUpdateReqV1(
                route_id=self.route_id,
                updates=skfs_action,
                timestamp=int(time.time()),
                signer=self.delegate_keypair.address.bin
            )
            req.signature = self.delegate_keypair.sign(req.SerializeToString())
            resp = await service.update_skfs(req)
        return json.dumps(resp.to_dict(), indent=2)

    async def route_skfs_list(self) -> list[dict]:
        """get all skfs assicated with a route id"""
        async with Channel(self.helium_host, self.helium_port) as channel:
            service = iot_config.RouteStub(channel)
            req = iot_config.RouteSkfListReqV1(
                route_id=self.route_id,
                timestamp=int(time.time()),
                signer=self.delegate_keypair.address.bin
            )
            req.signature = self.delegate_keypair.sign(req.SerializeToString())
            all_skfs = []
            async for skfs in service.list_skfs(req):
                device = skfs.to_dict()
                device['devaddr'] = hex(device['devaddr'])[2:]
                if 'maxCopies' not in device.keys():
                    device['maxCopies'] = 0
                all_skfs.append(device)
        return all_skfs

    async def route_skfs_devaddr(self, devaddr) -> list[dict]:
        """get skfs only assicated to a devaddr"""
        async with Channel(self.helium_host, self.helium_port) as channel:
            service = iot_config.RouteStub(channel)
            req = iot_config.RouteSkfGetReqV1(
                route_id=self.route_id,
                devaddr=devaddr,
                timestamp=int(time.time()),
                signer=self.delegate_keypair.address.bin
            )
            req.signature = self.delegate_keypair.sign(req.SerializeToString())
            all_skfs = []
            async for skfs in service.list_skfs(req):
                device = skfs.to_dict()
                device['devaddr'] = hex(device['devaddr'])[2:]
                if 'maxCopies' not in device.keys():
                    device['maxCopies'] = 0
                all_skfs.append(device)
        return all_skfs
