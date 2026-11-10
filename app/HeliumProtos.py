import os
import asyncio
import json
import time
import logging
import grpc
import nacl.bindings
from helium_py.crypto.keypair import Keypair
from helium_py.crypto.keypair import SodiumKeyPair
from protos.helium import iot_config
from grpclib.client import Channel
from models import device_row
from api import get_device_data
from schemas import GetRouteSkfsList, GetDeviceSyncRequest


# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
# HELIUM gRPC API CALLS
# ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
class HeliumConfigCli:
    def __init__(self, database):
        self.helium_host = os.getenv('HELIUM_HOST', default='mainnet-config.helium.io')
        self.helium_port = int(os.getenv('HELIUM_PORT', default=6080))
        self.helium_oui = int(os.getenv('HELIUM_OUI', default=None))
        self.route_id = os.getenv('ROUTE_ID', None)
        self.database = database
        # One instance per route/process. Serialize fresh reads through remote writes.
        self.sync_lock = asyncio.Lock()
        self.delegate_key = r'/app/delegate_key.bin'
        self._channel = None

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


    async def _get_channel(self) -> Channel:
        if self._channel is None:
            self._channel = Channel(self.helium_host, self.helium_port)
        return self._channel


    def close_channel(self):
        """Call this once when the script shuts down."""
        if self._channel is not None:
            self._channel.close()


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
        channel = await self._get_channel()
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
        resp = await service.update_euis([req], timeout=15)
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
        channel = await self._get_channel()
        service = iot_config.RouteStub(channel)
        req = iot_config.RouteSkfUpdateReqV1(
            route_id=self.route_id,
            updates=skfs_action,
            timestamp=int(time.time()),
            signer=self.delegate_keypair.address.bin
        )
        req.signature = self.delegate_keypair.sign(req.SerializeToString())
        resp = await service.update_skfs(req, timeout=15)
        print(json.dumps(resp.to_dict(include_default_values=True), indent=2))
        print('^ ============ ^ SESSION KEY -> HPR SYNC ^ ============ ^')
        return


    async def route_skfs_list(self) -> list[dict]:
        """get all skfs assicated with a helium route id"""
        channel = await self._get_channel()
        service = iot_config.RouteStub(channel)
        req = iot_config.RouteSkfListReqV1(
            route_id=self.route_id,
            timestamp=int(time.time()),
            signer=self.delegate_keypair.address.bin
        )
        req.signature = self.delegate_keypair.sign(req.SerializeToString())
        all_skfs = []
        async for skfs in service.list_skfs(req, timeout=30):
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
        channel = await self._get_channel()
        service = iot_config.RouteStub(channel)
        req = iot_config.RouteSkfGetReqV1(
            route_id=self.route_id,
            devaddr=devaddr,
            timestamp=int(time.time()),
            signer=self.delegate_keypair.address.bin
        )
        req.signature = self.delegate_keypair.sign(req.SerializeToString())
        all_skfs = []
        async for skfs in service.list_skfs(req, timeout=30):
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
    @staticmethod
    def skf_update(device):
        return iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
            devaddr=device.devAddr,
            session_key=device.nwkSEncKey,
            action=iot_config.ActionV1(1 if device.isDisabled or device.is_private else 0),
            max_copies=device.max_copies,
        )

    async def sync_device(self, dev_eui, update_euis=False):
        # Lock before the read: locking only the write would still allow stale actions.
        async with self.sync_lock:
            try:
                device, observed_at = await get_device_data(dev_eui, use_cache=False)
            except grpc.aio.AioRpcError as err:
                if err.code() != grpc.StatusCode.NOT_FOUND:
                    raise
                # Retained create/update/join events can outlive the device itself.
                print(f'Skipping event for deleted device {dev_eui}')
                return
            d = GetDeviceSyncRequest(**device)
            await self.database.upsert_device([device_row(d, observed_at, self.route_id)])
            if update_euis:
                await self.route_euis(d.devEui, d.joinEui, int(d.isDisabled or d.is_private))
            if d.nwkSEncKey:
                await self.route_skfs([self.skf_update(d)])

    async def add_device_euis(self, meta):
        await self.sync_device(meta['dev_eui'], update_euis=True)

    async def remove_device_euis(self, meta):
        async with self.sync_lock:
            dev_eui, join_eui = await self.database.get_device_euis(meta['dev_eui'])
            await self.route_euis(dev_eui, join_eui, 1)

    async def update_device(self, meta):
        await self.sync_device(meta['dev_eui'], update_euis=True)

    async def remove_stale_skfs(self, sync_state):
        if not sync_state.complete:
            return 0
        stale_skfs = await self.database.get_stale_skfs(self.route_id)
        removed = 0
        for group in self.chunker(stale_skfs, 100):
            async with self.sync_lock:
                # A refresh may have started while we were waiting for this lock.
                if not sync_state.complete:
                    break
                still_stale = set(await self.database.filter_still_stale(
                    [skf['sessionKey'] for skf in group]
                ))
                updates = [
                    iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                        devaddr=int(skf['devaddr']),
                        session_key=skf['sessionKey'],
                        action=iot_config.ActionV1(1),
                    )
                    for skf in group if skf['sessionKey'] in still_stale
                ]
                if updates:
                    await self.route_skfs(updates)
                    await self.database.delete_helium_skfs(self.route_id, list(still_stale))
                    removed += len(updates)
        return removed
