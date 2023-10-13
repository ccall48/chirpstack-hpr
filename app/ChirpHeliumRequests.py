import os
import subprocess
import psycopg2
import psycopg2.extras
import redis
import grpc
from google.protobuf.json_format import MessageToJson, MessageToDict
from chirpstack_api import api


# -----------------------------------------------------------------------------
# CHIRPSTACK REDIS CONNECTION
# -----------------------------------------------------------------------------
redis_server = os.getenv('REDIS_HOST')
rpool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=rpool, decode_responses=True)


class ChirpstackStreams:
    def __init__(
            self,
            route_id: str,
            postgres_host: str,
            postgres_user: str,
            postgres_pass: str,
            postgres_name: str,
            chirpstack_host: str,
            chirpstack_token: str,
    ):
        self.route_id = route_id
        self.pg_host = postgres_host
        self.pg_user = postgres_user
        self.pg_pass = postgres_pass
        self.pg_name = postgres_name
        self.postges = f'postgresql://{self.pg_user}:{self.pg_pass}@{self.pg_host}/{self.pg_name}'
        self.cs_gprc = chirpstack_host
        self.auth_token = [('authorization', f'Bearer {chirpstack_token}')]

    def config_service_cli(self, cmd: str):
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        if err:
            return err
        print(out)
        return out

    def db_transaction(self, query):
        with psycopg2.connect(self.postges) as con:
            with con.cursor() as cur:
                cur.execute(query)

    def db_fetch(self, query):
        with psycopg2.connect(self.postges) as con:
            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()

    def db_test_query(self, query):
        with psycopg2.connect(self.postges) as con:
            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                result = cur.fetchall()
                if len(result) > 0:
                    for row in result:
                        print(row)
                else:
                    print('No Results')

    def get_device_request(self, dev_eui: str):
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = client.Get(req, metadata=self.auth_token)
            data = MessageToDict(resp)['device']
        return data['devEui'], data['joinEui']

    def get_device_request_data(self, dev_eui: str):
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = client.Get(req, metadata=self.auth_token)
            data = MessageToDict(resp)['device']
        return data

    def get_device_activation(self, dev_eui: str):
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceActivationRequest()
            req.dev_eui = dev_eui
            resp = client.GetActivation(req, metadata=self.auth_token)
            data = MessageToDict(resp)['deviceActivation']
        return data

    def create_tables(self):
        query = """
            CREATE TABLE IF NOT EXISTS helium_devices (
                dev_eui text primary key,           -- devices['devEui']
                join_eui text,                      -- devices['joinEui']
                dev_addr text,                      -- devices['devAddr']
                max_copies int default 0,           -- set as configuration variable
                aps_key text,                       -- devices['appSKey']
                nws_key text,                       -- devices['nwkSEncKey']
                dev_name text,                      -- devices['name']
                fcnt_up int,                        -- devices['fCntUp']
                fcnt_down int,                      -- devices['nFCntDown']
                dc_used int default 0,              -- 2_147_483_647 int max
                is_disabled bool default false
            );
        """
        print('Run create helium device table if not exists.')
        self.db_transaction(query)

    def fetch_active_devices(self) -> list[str]:
        query = "SELECT dev_eui FROM device WHERE is_disabled=false;"
        result = [device['dev_eui'].hex() for device in self.db_fetch(query)]
        return result

    def api_stream_requests(self):
        stream_key = "api:stream:request"
        last_id = '0'
        while True:
            try:
                resp = rdb.xread({stream_key: last_id}, count=1, block=0)

                for message in resp[0][1]:
                    last_id = message[0]

                    if b'request' in message[1]:
                        msg = message[1][b'request']
                        pl = api.request_log_pb2.RequestLog()
                        pl.ParseFromString(msg)
                        req = MessageToDict(pl)
                        if 'method' not in req.keys():
                            continue

                        match req['service']:
                            case 'api.DeviceService':
                                if req['method'] == 'Create':
                                    print('========== API Create Euis ==========')
                                    print(MessageToJson(pl))
                                    self.add_device_euis(req['metadata'])

                                if req['method'] == 'Delete':
                                    print('========== API Delete Euis ==========')
                                    print(MessageToJson(pl))
                                    self.remove_device_euis(req['metadata'])

                                if req['method'] == 'Update':
                                    print('========== API Update Euis ==========')
                                    print(MessageToJson(pl))
                                    self.update_device_euis(req['metadata'])

            except Exception as err:
                print(f'api_stream_requests: {err}')
                pass

    def add_device_euis(self, data: dict):
        """
        On device being added using chirpstack webui or api
            - add device euis to hpr
            - add device to helium_devices db
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        dev_eui, join_eui = self.get_device_request(device)
        print(f'Add Device EUIs: {dev_eui}, {join_eui}')

        query = """
            INSERT INTO helium_devices (dev_eui, join_eui)
            VALUES ('{0}', '{1}')
            ON CONFLICT (dev_eui) DO NOTHING;
        """.format(dev_eui, join_eui)
        self.db_transaction(query)

        cmd = f'hpr route euis add -d {dev_eui} -a {join_eui} --route-id {self.route_id} --commit'
        self.config_service_cli(cmd)
        print('==[ ADD EUIS ]==>')
        return

    def remove_device_euis(self, data: dict):
        """
        On device being removed using chirpstack webui or api.
            - call dev_addr and nws_keys to be removed from hpr skfs
            - call device from helium_devices db on delete and remove from hpr device euis
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        print(f'Remove Device: {device}')

        query = "SELECT * FROM helium_devices WHERE dev_eui='{}';".format(device)
        data = self.db_fetch(query)[0]

        # if data['dev_addr'] is not None and data['nws_key'] is not None:
        if data['dev_addr'] != '' and data['nws_key'] != '':
            dev_addr = data['dev_addr']  # this should be a string
            nws_key = data['nws_key']    # this should be a string
            # if set remove dev_addr and nws_key from skfs's
            cmd = f'hpr route skfs remove -r {self.route_id} -d {dev_addr} -s {nws_key} -c'
            self.config_service_cli(cmd)
            print(f'Removing SKFS -> {cmd}')

        dev_eui = data['dev_eui']    # this should be a string
        join_eui = data['join_eui']  # this should be a string
        # remove euis, device eui and join eui for device from router
        cmd = f'hpr route euis remove -d {dev_eui} -a {join_eui} --route-id {self.route_id} -c'
        self.config_service_cli(cmd)
        print(f'Removing EUIS -> {cmd}')
        # delete or disable device in helium_device table.
        return

    def update_device_euis(self, data: dict):
        """
        On device being disabled in chirpstack webui or api
            - remove device euis on disable toggle from hpr
            - add device euis to hpr on enable toggle
            - update device device status to is_disabled in helium_devices
            - update max_copies
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        is_disabled = data['is_disabled']
        dev_eui, join_eui = self.get_device_request(device)
        if is_disabled == 'true':
            cmd = f'hpr route euis remove -d {dev_eui} -a {join_eui} --route-id {self.route_id} -c'
            query = """
                UPDATE helium_devices SET is_disabled=true WHERE dev_eui='{}';
            """.format(dev_eui)
            self.db_transaction(query)
        else:
            cmd = f'hpr route euis add -d {dev_eui} -a {join_eui} --route-id {self.route_id} -c'
            query = """
                UPDATE helium_devices SET is_disabled=false WHERE dev_eui='{}';
            """.format(dev_eui)
            self.db_transaction(query)
        self.config_service_cli(cmd)
        print('==[ UPDATE EUIS debug... ]==>')
        return
