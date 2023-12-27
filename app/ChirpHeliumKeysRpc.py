from functools import wraps
import psycopg2
import psycopg2.extras
import grpc
from google.protobuf.json_format import MessageToDict
from chirpstack_api import api
import logging

from ChirpHeliumCrypto import get_route_skfs, update_device_skfs
from protos.helium import iot_config


# def my_logger(orig_func):
#     logging.basicConfig(
#         filename='chirpstack-hpr.log',
#         filemode='a',
#         format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
#         level=logging.INFO,
#         datefmt='%Y-%m-%d %H:%M:%S',
#     )
#     logging.getLogger("asyncio").setLevel(logging.INFO)
#
#     @wraps(orig_func)
#     def wrapper(*args, **kwargs):
#         logging.info(
#             f'Passed args: {args}, kwargs: {kwargs}')
#         return orig_func(*args, **kwargs)
#     return wrapper


class ChirpDeviceKeys:
    def __init__(
        self,
        route_id: str,
        postgres_host: str,
        postgres_user: str,
        postgres_pass: str,
        postgres_name: str,
        postgres_port: str,
        postgres_ssl_mode: str,
        chirpstack_host: str,
        chirpstack_token: str,
    ):
        self.route_id = route_id
        self.pg_host = postgres_host
        self.pg_user = postgres_user
        self.pg_pass = postgres_pass
        self.pg_name = postgres_name
        self.pg_port = postgres_port
        self.pg_ssl_mode = postgres_ssl_mode
        conn_str = f"postgresql://{self.pg_user}:{self.pg_pass}@{self.pg_host}:{self.pg_port}/{self.pg_name}"
        if self.pg_ssl_mode[0] != "require":
            self.postgres = conn_str
        else:
            self.postgres = "%s?sslmode=%s" % (conn_str, self.pg_ssl_mode)
        self.cs_gprc = chirpstack_host
        self.auth_token = [("authorization", f"Bearer {chirpstack_token}")]

    def db_fetch(self, query: str):
        with psycopg2.connect(self.postgres) as con:
            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()

    def db_transaction(self, query: str):
        with psycopg2.connect(self.postgres) as con:
            with con.cursor() as cur:
                cur.execute(query)

    def fetch_all_devices(self) -> list[str]:
        with psycopg2.connect(self.postgres) as con:
            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT encode(dev_eui, 'hex') AS dev_eui
                    FROM device
                    WHERE is_disabled=false;
                    """
                )
                return [dev['dev_eui'] for dev in cur.fetchall()]

    def chunker(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    ###########################################################################
    # Chirpstack gRPC API calls
    ###########################################################################
    # @my_logger
    def get_device(self, dev_eui: str) -> dict[str]:
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = client.Get(req, metadata=self.auth_token)
            data = MessageToDict(resp)["device"]
        return data

    # @my_logger
    def get_device_activation(self, dev_eui: str) -> dict[str]:
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceActivationRequest()
            req.dev_eui = dev_eui
            resp = client.GetActivation(req, metadata=self.auth_token)
            data = MessageToDict(resp)
            if bool(data):
                return data["deviceActivation"]
        return data

    # @my_logger
    def get_merged_keys(self, dev_eui: str) -> dict[str]:
        devices = {
            "devAddr": "",
            "appSKey": "",
            "nwkSEncKey": "",
            "name": "",
        }

        devices.update(self.get_device(dev_eui))
        devices.update(self.get_device_activation(dev_eui))

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
        self.db_transaction(query)
        return f"Updated: {dev_eui}"

    # @my_logger
    async def helium_skfs_update(self):
        """
        TODO:
            run function on a device join success, or on a device update.
        """
        helium_devices = """
            SELECT dev_addr, nws_key, max_copies
            FROM helium_devices
            WHERE is_disabled=false
            AND dev_addr != '';
        """
        all_helium_devices = self.db_fetch(helium_devices)

        skfs_list = await get_route_skfs()

        # logging.info(f"All Helium Devices: {all_helium_devices}")
        # logging.info(f"SKFS List: {skfs_list}")

        # Convert the lists to sets for efficient set operations
        # compare dev_addr & session_key for match else remove
        all_helium_sessions_set = {
            (d["dev_addr"], d["nws_key"]) for d in all_helium_devices
        }

        all_skfs_sessions_set = {
            (d["devaddr"], d["sessionKey"]) for d in skfs_list
        }

        devices_to_remove = all_helium_sessions_set ^ all_skfs_sessions_set

        # only update max_copies if changed, do not remove and re add if only max copies changes
        # as it seems to make the session key hang and not pass data.
        all_helium_devices_set = {
            (d["dev_addr"], d["nws_key"], d["max_copies"])
            for d in all_helium_devices
        }

        skfs_list_set = {
            (d["devaddr"], d["sessionKey"], d["maxCopies"])
            for d in skfs_list
        }

        # logging.info(f"All Helium Devices Set: {all_helium_devices_set}")
        # logging.info(f"SKFS List Set: {skfs_list_set}")

        # Devices to add to skfs_list
        devices_to_add = all_helium_devices_set - skfs_list_set

        logging.info(f"Devices_to_remove: {devices_to_remove}")
        logging.info(f"Devices_to_add: {devices_to_add}")

        # with rpc we can make update to a max of 100 skfs in one request
        rm_skfs = []
        add_skfs = []

        if devices_to_remove:
            rm_skfs = [
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=int(dev_addr, 16),
                    session_key=nws_key,
                    action=iot_config.ActionV1(1)
                ) for dev_addr, nws_key in devices_to_remove
            ]
            # logging.info(f'RM-SKFS: {rm_skfs}')

        if devices_to_add:
            add_skfs = [
                iot_config.RouteSkfUpdateReqV1RouteSkfUpdateV1(
                    devaddr=int(dev_addr, 16),
                    session_key=nws_key,
                    action=iot_config.ActionV1(0),
                    max_copies=max_copies
                ) for dev_addr, nws_key, max_copies in devices_to_add
            ]
            # logging.info(f'ADD-SKFS: {add_skfs}')

        skfs_action = rm_skfs + add_skfs

        if skfs_action:
            """Chunk updates max of 100 requests at a time, helium rpc restriction"""
            for group in self.chunker(skfs_action, 100):
                logging.info(f'Chunked_Update: {group}')
                skfs_update_chunk = await update_device_skfs(self.route_id, group)
                logging.info(f'skfs_to_update: {skfs_update_chunk}')

        return "Updated SKFS"
