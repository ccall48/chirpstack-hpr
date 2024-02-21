import os
from datetime import datetime
from functools import wraps
import ujson
import logging

import nacl.bindings
from helium_py.crypto.keypair import Keypair
from helium_py.crypto.keypair import SodiumKeyPair

from protos.helium import iot_config
from grpclib.client import Channel

# precision for signing rpc's moving from milliseconds to seconds in next iot config update
# quick helper function to switch it over quickly when updated.
PRECISION = 's'

host = os.getenv("HELIUM_HOST", default="mainnet-config.helium.io")
port = int(os.getenv("HELIUM_PORT", default=6080))
oui = int(os.getenv("HELIUM_OUI", default=None))
route_id = os.getenv('ROUTE_ID', None)
delegate_key = os.getenv('HELIUM_KEYPAIR_BIN', default=None)

with open(delegate_key, 'rb') as f:
    skey = f.read()[1:]
    delegate_keypair = Keypair(
        SodiumKeyPair(
            sk=skey,
            pk=nacl.bindings.crypto_sign_ed25519_sk_to_pk(skey)
        )
    )


def my_logger(orig_func):
    logging.basicConfig(
        filename='chirpstack-hpr.log',
        filemode='a',
        format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.getLogger("asyncio").setLevel(logging.INFO)

    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        logging.info(
            f'Passed args: {args}, kwargs: {kwargs}')
        return orig_func(*args, **kwargs)
    return wrapper


def rpc_time(precision: str = None):
    if precision == 'ms':
        return int(datetime.utcnow().timestamp()*1000)
    elif precision == 's':
        return int(datetime.utcnow().timestamp())


###########################################################################
# Helium gRPC API calls
###########################################################################
@my_logger
async def sync_device_euis(action: bool, app_eui: str, dev_eui: str, route_id: str):
    app_eui = int(app_eui, 16)
    dev_eui = int(dev_eui, 16)
    print(f'dev_eui: {dev_eui}, app_eui: {app_eui}')
    async with Channel(host, port) as channel:
        service = iot_config.RouteStub(channel)
        req = iot_config.RouteUpdateEuisReqV1(
            action=iot_config.ActionV1(action),
            eui_pair=iot_config.EuiPairV1(
                route_id=route_id,
                app_eui=app_eui,
                dev_eui=dev_eui
            ),
            timestamp=rpc_time(PRECISION),
            signer=delegate_keypair.address.bin
        )
        req.signature = delegate_keypair.sign(req.SerializeToString())
        resp = await service.update_euis([req])
    print(ujson.dumps(resp.to_dict(), indent=2))
    return


@my_logger
async def get_route_skfs() -> list[dict]:
    async with Channel(host, port) as channel:
        service = iot_config.RouteStub(channel)
        req = iot_config.RouteSkfListReqV1(
            route_id=route_id,
            timestamp=rpc_time(PRECISION),
            signer=delegate_keypair.address.bin
        )
        req.signature = delegate_keypair.sign(req.SerializeToString())
        all_skfs = []
        async for skfs in service.list_skfs(req):
            device = skfs.to_dict()
            device['devaddr'] = hex(device['devaddr'])[2:]
            if 'maxCopies' not in device.keys():
                device['maxCopies'] = 0
            all_skfs.append(device)
    return all_skfs


@my_logger
async def update_device_skfs(route_id: str, skfs_action: list):
    """ Example of device session key update.
        skfs_action: list ->
        [
            iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                devaddr=`uint32`,
                session_key=`str`,
                action=iot_config.ActionV1(`enum`),
                max_copies=`uint32`
            ),
            ...
        ]
    """
    async with Channel(host, port) as channel:
        service = iot_config.RouteStub(channel)
        req = iot_config.RouteSkfUpdateReqV1(
            route_id=route_id,
            updates=skfs_action,
            timestamp=rpc_time(PRECISION),
            signer=delegate_keypair.address.bin
        )
        req.signature = delegate_keypair.sign(req.SerializeToString())
        resp = await service.update_skfs(req)
    return ujson.dumps(resp.to_dict(), indent=2)
