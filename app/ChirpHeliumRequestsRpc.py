import os
from functools import wraps
import json
from datetime import datetime
import psycopg2
import psycopg2.extras
import redis.asyncio as redis
import grpc
from google.protobuf.json_format import MessageToJson, MessageToDict
from chirpstack_api import api
import logging

from ChirpHeliumCrypto import sync_device_euis, update_device_skfs


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
        conn_str = f'postgresql://{self.pg_user}:{self.pg_pass}@{self.pg_host}:{self.pg_port}/{self.pg_name}'
        if self.pg_ssl_mode[0] != 'require':
            self.postgres = conn_str
        else:
            self.postgres = '%s?sslmode=%s' % (conn_str, self.pg_ssl_mode)
        self.cs_gprc = chirpstack_host
        self.auth_token = [('authorization', f'Bearer {chirpstack_token}')]

    ###########################################################################
    # functions to handle helium device db transactions
    ###########################################################################
    def db_transaction(self, query):
        with psycopg2.connect(self.postgres) as con:
            with con.cursor() as cur:
                cur.execute(query)

    def db_fetch(self, query):
        with psycopg2.connect(self.postgres) as con:
            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()

    def fetch_active_devices(self) -> list[str]:
        query = "SELECT dev_eui FROM device WHERE is_disabled=false;"
        result = [device['dev_eui'].hex() for device in self.db_fetch(query)]
        return result

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

    ###########################################################################
    # Chirpstack gRPC API calls
    ###########################################################################
    async def get_device_request(self, dev_eui: str):
        async with grpc.aio.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = await client.Get(req, metadata=self.auth_token)
            data = MessageToDict(resp)['device']
        return data['devEui'], data['joinEui']

    async def get_device_request_data(self, dev_eui: str):
        async with grpc.aio.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = await client.Get(req, metadata=self.auth_token)
            data = MessageToDict(resp)['device']
        return data

    async def get_device_activation(self, dev_eui: str):
        async with grpc.aio.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceActivationRequest()
            req.dev_eui = dev_eui
            resp = await client.GetActivation(req, metadata=self.auth_token)
            data = MessageToDict(resp)['deviceActivation']
        return data

    ###########################################################################
    # follow internal redis stream gRPC for actionable changes
    ###########################################################################
    async def api_stream_requests(self):
        stream_key = "api:stream:request"
        last_id = '0'
        while True:
            try:
                resp = await rdb.xread({stream_key: last_id}, count=1, block=0)

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
                                    await self.add_device_euis(req['metadata'])

                                if req['method'] == 'Delete':
                                    print('========== API Delete Euis ==========')
                                    print(MessageToJson(pl))
                                    await self.remove_device_euis(req['metadata'])

                                if req['method'] == 'Update':
                                    print('========== API Update Euis ==========')
                                    print(MessageToJson(pl))
                                    await self.update_device_euis(req['metadata'])

            except Exception as err:
                logging.info(f'api_stream_requests: {err}')
                pass

    @my_logger
    async def add_device_euis(self, data: dict):
        """
        On device being added using chirpstack webui or api
            - add device euis to hpr
            - add device to helium_devices db
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        dev_eui, join_eui = await self.get_device_request(device)
        print(f'Add Device EUIs: {dev_eui}, {join_eui}')

        query = """
            INSERT INTO helium_devices (dev_eui, join_eui)
            VALUES ('{0}', '{1}')
            ON CONFLICT (dev_eui) DO NOTHING;
        """.format(dev_eui, join_eui)
        self.db_transaction(query)

        await sync_device_euis(0, join_eui, dev_eui, self.route_id)
        return

    @my_logger
    async def remove_device_euis(self, data: dict):
        """
        On device being removed using chirpstack webui or api.
            - call dev_addr and nws_keys to be removed from hpr skfs if activated.
            - call device from helium_devices db on delete and remove from hpr device euis
            - remove device from helium_devices db
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        print(f'Remove Device: {device}')

        query = "SELECT * FROM helium_devices WHERE dev_eui='{}';".format(device)
        data = self.db_fetch(query)[0]

        #if data['dev_addr'] is not None:
        #    dev_addr = data['dev_addr']  # this should be a string
        #    nws_key = data['nws_key']    # this should be a string
        #    # if set remove dev_addr and nws_key from skfs's
        #    rm_skfs = f'hpr route skfs remove -r {self.route_id} -d {dev_addr} -s {nws_key} -c'
        #    self.config_service_cli(rm_skfs)
        #    print(f'Removing SKFS -> {rm_skfs}')

        dev_eui = data['dev_eui']    # this should be a string
        join_eui = data['join_eui']  # this should be a string
        action = 1
        # remove euis, device eui and join eui for device from router
        print('remove-device:', action, join_eui, dev_eui, self.route_id)
        await sync_device_euis(action, join_eui, dev_eui, self.route_id)

        # add in remove device skfs here in lookup?

        # delete or disable device in helium_device table.
        delete_device = """
            DELETE FROM helium_devices WHERE dev_eui='{}';
        """.format(device)
        self.db_transaction(delete_device)
        return

    @my_logger
    async def update_device_euis(self, data: dict):
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
        dev_eui, join_eui = await self.get_device_request(device)
        if is_disabled == 'true':
            action = 1
            query = """
                UPDATE helium_devices SET is_disabled=true WHERE dev_eui='{}';
            """.format(dev_eui)
            self.db_transaction(query)

        elif is_disabled == 'false':
            action = 0
            query = """
                UPDATE helium_devices SET is_disabled=false WHERE dev_eui='{}';
            """.format(dev_eui)
            self.db_transaction(query)
        print('action', action, 'join', join_eui, 'dev', dev_eui)
        await sync_device_euis(action, join_eui, dev_eui, self.route_id)
        return
