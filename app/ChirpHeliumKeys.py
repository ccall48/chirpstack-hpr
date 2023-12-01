import subprocess
import psycopg2
import psycopg2.extras
import ujson
import grpc
from google.protobuf.json_format import MessageToDict
from chirpstack_api import api
import logging


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

    def config_service_cli(self, cmd: str):
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        if err:
            return err
        return out

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
                cur.execute("SELECT dev_eui FROM device WHERE is_disabled=false;")
                return [dev["dev_eui"].hex() for dev in cur.fetchall()]

    def get_device(self, dev_eui: str) -> dict[str]:
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = client.Get(req, metadata=self.auth_token)
            data = MessageToDict(resp)["device"]
        return data

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

    def helium_skfs_update(self):
        """
        TODO:
            run function on a device join success, or on a device update.
        """
        helium_devices_query = """
            SELECT dev_addr, nws_key, max_copies
            FROM helium_devices
            WHERE is_disabled=false
            AND dev_addr != '';
        """
        all_helium_devices = self.db_fetch(helium_devices_query)

        cmd = f"hpr route skfs list --route-id {self.route_id}"
        skfs_list = ujson.loads(self.config_service_cli(cmd))

        logging.info(f"SKFS List: {skfs_list}")
        logging.info(f"All Helium Devices: {all_helium_devices}")

        all_helium_devices_set = {
            (d["dev_addr"], d["nws_key"]) for d in all_helium_devices
        }
        skfs_list_set = {(d["dev_addr"], d["nws_key"]) for d in skfs_list}

        logging.info(f"SKFS List Set: {skfs_list_set}")
        logging.info(f"All Helium Devices Set: {all_helium_devices_set}")

        devices_to_remove = skfs_list_set - all_helium_devices_set
        devices_to_add = all_helium_devices_set - skfs_list_set

        logging.info(f"Devices_to_remove: {devices_to_remove}")
        logging.info(f"Devices_to_add: {devices_to_add}")

        remove_cmds = [
            f"hpr route skfs remove --route-id {self.route_id} --dev-addr {dev_addr} --nws-key {nws_key}"
            for dev_addr, nws_key in devices_to_remove
        ]
        add_cmds = [
            f"hpr route skfs add --route-id {self.route_id} --dev-addr {dev_addr} --nws-key {nws_key}"
            for dev_addr, nws_key in devices_to_add
        ]

        for cmd in remove_cmds:
            self.config_service_cli(cmd)
        for cmd in add_cmds:
            self.config_service_cli(cmd)
