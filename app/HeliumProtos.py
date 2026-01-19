import os
import json
import time
import logging
import nacl.bindings
from helium_py.crypto.keypair import Keypair
from helium_py.crypto.keypair import SodiumKeyPair
from protos.helium import iot_config
from grpclib.client import Channel
from models import DeviceDatabase
from api import (
    get_device_euis,
    get_device_data,
)
from schemas import GetRouteSkfsList, GetDeviceSyncRequest


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# HELIUM gRPC API CALLS
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
class HeliumConfigCli:
    def __init__(self):
        self.helium_host = os.getenv('HELIUM_HOST', default='mainnet-config.helium.io')
        self.helium_port = int(os.getenv('HELIUM_PORT', default=6080))
        self.helium_oui = int(os.getenv('HELIUM_OUI', default=None))
        self.route_id = os.getenv('ROUTE_ID', None)
        self.database = DeviceDatabase()
        self.delegate_key = r'/app/delegate_key.bin'

        with open(self.delegate_key, 'rb') as f:
            blob = f.read()[:65]
            key_net_and_type, skey = blob[0], blob[1:65]
            KEY_TYPE_ED25519 = 1
            if (key_net_and_type & 0x0f) != KEY_TYPE_ED25519:
                # The Helium blockchain historically supported two
                # different key types: Ed25519 and ECC Compact.
                # ECC Compact requires different code, which we don't have
                # at the moment.
                warning = \
                    "Unsupported delegate private key type. Only Ed25519 " \
                    "keys are supported."
                logging.error(warning)
                raise Exception(warning)

            self.delegate_keypair = Keypair(
                SodiumKeyPair(
                    sk=skey,
                    pk=nacl.bindings.crypto_sign_ed25519_sk_to_pk(skey)
                )
            )


    def chunker(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))


    async def route_euis(self, dev_eui: str, join_eui: str, action: bool):
        """ Example device euis update, pairs to be sent as integers
            euis_action: list ->
            [
                iot_config.RouteUpdateEuisReqV1(
                    action=iot_config.ActionV1(`enum`), # 0 add, 1 remove
                    eui_pair=iot_config.EuiPairV1(
                        route_id=`uuid`,
                        app_eui=`uint32`,
                        dev_eui=`uint32`,
                ),
                ...
            ]
        """
        async with Channel(self.helium_host, self.helium_port) as channel:
            service = iot_config.RouteStub(channel)
            req = iot_config.RouteUpdateEuisReqV1(
                action=iot_config.ActionV1(action),
                eui_pair=iot_config.EuiPairV1(
                    route_id=self.route_id,
                    app_eui=join_eui,
                    dev_eui=dev_eui,
                ),
                timestamp=int(time.time()),
                signer=self.delegate_keypair.address.bin
            )
            req.signature = self.delegate_keypair.sign(req.SerializeToString())
            resp = await service.update_euis([req])
        print(json.dumps(resp.to_dict(include_default_values=True), indent=2))
        return


    async def route_skfs(self, skfs_action: list):
        """ Example of device session key update.
            skfs_action: list ->
            [
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=`uint32`,
                    session_key=`str`,
                    action=iot_config.ActionV1(`enum`),  # 0 add, 1 remove
                    max_copies=`uint32`  # not required for removal
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
        print(json.dumps(resp.to_dict(include_default_values=True), indent=2))
        return


    async def route_skfs_list(self) -> list[dict]:
        """get all skfs assicated with a helium route id"""
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
                d = GetRouteSkfsList(**skfs.to_dict(include_default_values=True))
                device = {
                    'routeId': d.routeId,
                    'devaddr': d.devaddr,
                    'sessionKey': d.sessionKey,
                    'maxCopies': d.maxCopies
                }
                all_skfs.append(device)
        return all_skfs


    async def route_skfs_devaddr(self, devaddr) -> list[dict]:
        """get skfs assicated with a single devaddr"""
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
                d = GetRouteSkfsList(**skfs.to_dict(include_default_values=True))
                device = {
                    'routeId': d.routeId,
                    'devaddr': d.devaddr,
                    'sessionKey': d.sessionKey,
                    'maxCopies': d.maxCopies
                }
                all_skfs.append(device)
        return all_skfs


    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
    # add / remove device euis from HPR
    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
    async def add_device_euis(self, meta):
        action = 0
        device = meta['dev_eui']
        dev_eui, join_eui = await get_device_euis(device)
        return await self.route_euis(int(dev_eui, 16), int(join_eui, 16), action)


    async def remove_device_euis(self, meta):
        action = 1
        device = meta['dev_eui']
        dev_eui, join_eui = await self.database.get_device_euis(device)
        return await self.route_euis(dev_eui, join_eui, action)


    async def update_device(self, meta):
        """
        {
            "service": "api.DeviceService",
            "method": "Update",
            "metadata": {
                "dev_eui": "2cf7f1c053800309",
                "is_disabled": "false"
            }
        }
        """
        device = await get_device_data(meta['dev_eui'])
        d = GetDeviceSyncRequest(**device)

        # If device privacy not set, assume false and full roaming
        is_private = d.variables.get('private', False)

        if d.isDisabled or is_private:
            # remove euis - action = 1
            action = 1
            await self.route_euis(d.devEui, d.joinEui, action)
            # remove skfs
            skfs_to_remove = [
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=d.devAddr,
                    session_key=d.nwkSEncKey,
                    action=iot_config.ActionV1(1),
                )
            ]
            await self.route_skfs(skfs_to_remove)

        elif not d.isDisabled:
            # add euis - action = 0
            action = 0
            await self.route_euis(d.devEui, d.joinEui, action)
            # sync skfs with max_copies update
            skfs_to_update = [
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=d.devAddr,
                    session_key=d.nwkSEncKey,
                    action=iot_config.ActionV1(0),
                    max_copies=d.variables.get('max_copies', 0)
                )
            ]
            await self.route_skfs(skfs_to_update)


    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
    # Purge stale skfs
    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
    async def remove_stale_skfs(self):
        stale_skfs_list = await self.database.get_stale_skfs()
        skfs_to_remove = []
        for skfs in stale_skfs_list:
            print(int(skfs['DevAddr']), skfs['sessionKey'])

            skfs_to_remove.append(
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=int(skfs['DevAddr']),
                    session_key=skfs['sessionKey'],
                    action=iot_config.ActionV1(1),
                )
            )

        if skfs_to_remove:
            # use chunker to limit update to max 100 per request
            for group in self.chunker(skfs_to_remove, 100):
                await self.route_skfs(group)
