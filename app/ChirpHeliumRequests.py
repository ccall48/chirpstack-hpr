import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import psycopg2.extras
# from paho.mqtt import client as paho_mqtt
# from math import ceil
import time
import redis
import grpc
from google.protobuf.json_format import MessageToJson
import ujson
from chirpstack_api import api, meta, integration


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
            data = ujson.loads(MessageToJson(resp))['device']
        return data['devEui'], data['joinEui']

    def get_device_request_data(self, dev_eui: str):
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceRequest()
            req.dev_eui = dev_eui
            resp = client.Get(req, metadata=self.auth_token)
            data = ujson.loads(MessageToJson(resp))['device']
        return data

    def get_device_activation(self, dev_eui: str):
        with grpc.insecure_channel(self.cs_gprc) as channel:
            client = api.DeviceServiceStub(channel)
            req = api.GetDeviceActivationRequest()
            req.dev_eui = dev_eui
            resp = client.GetActivation(req, metadata=self.auth_token)
            data = ujson.loads(MessageToJson(resp))['deviceActivation']
        return data

    def create_tables(self):
        query = """
            --CREATE TABLE IF NOT EXISTS helium_devices AS
            --SELECT
            --    application.tenant_id AS tenant_id,
            --    device.dev_addr AS dev_addr,
            --    device.dev_eui AS dev_eui,
            --    device.join_eui AS join_eui,
            --    device_keys.nwk_key AS nwk_key,
            --    (variables->>'max_copies')::int as max_copies,
            --    device.name AS device_name,
            --    device.is_disabled AS is_disabled
            --FROM device
            --JOIN device_keys
            --    ON device.dev_eui = device_keys.dev_eui
            --JOIN application
            --    ON device.application_id = application.id;
            --ALTER TABLE helium_devices ADD COLUMN IF NOT EXISTS nws_key text;
            --ALTER TABLE helium_devices ADD COLUMN IF NOT EXISTS fcnt_up int;
            --ALTER TABLE helium_devices ADD COLUMN IF NOT EXISTS fcnt_down int;
            --ALTER TABLE helium_devices ADD COLUMN IF NOT EXISTS is_added bool default false;

            CREATE TABLE IF NOT EXISTS helium_devices (
                id serial primary key,
                dev_eui text unique,    -- devices['devEui']
                join_eui text,          -- devices['joinEui']
                dev_addr text,          -- devices['devAddr']
                max_copies int,
                aps_key text,           -- devices['appSKey']
                nws_key text,           -- devices['nwkSEncKey']
                dev_name text,          -- devices['name']
                fcnt_up int,            -- devices['fCntUp']
                fcnt_down int,          -- devices['nFCntDown']
                is_disabled bool default false
            );

            CREATE TABLE IF NOT EXISTS helium_skfs (
                id serial primary key,
                dev_eui text unique,
                join_eui text,
                dev_addr text,
                nws_key text,
                max_copies int
            );

            CREATE TABLE IF NOT EXISTS helium_tenant (
                tenant_id uuid primary key,
                tenant_name text,
                dc_balance bigint default 1000,
                is_disabled bool default false
            );
        """
        print('Run create tables...')
        self.db_transaction(query)

    def update_tables(self):
        query = """
            --UPDATE helium_devices
            --SET
            --    dev_addr=device.dev_addr,
            --    dev_eui=device.dev_eui,
            --    join_eui=device.join_eui,
            --    max_copies=(variables->>'max_copies')::int,
            --    device_name=device.name,
            --    is_disabled=device.is_disabled
            --FROM device
            --WHERE helium_devices.dev_eui=device.dev_eui;

            --UPDATE helium_devices
            --SET
            --    nwk_key=device_keys.nwk_key
            --FROM device_keys
            --WHERE helium_devices.dev_eui=device_keys.dev_eui;

            INSERT INTO helium_tenant (tenant_id, tenant_name)
            SELECT tenant.id, tenant.name
            FROM tenant
            ON CONFLICT (tenant_id) DO NOTHING;
        """
        print(f'Run background tables update... {time.ctime()}')
        self.db_transaction(query)

    def fetch_active_devices(self) -> list[str]:
        query = "SELECT dev_eui FROM device WHERE is_disabled=false;"
        result = [d['dev_eui'].hex() for d in self.db_fetch(query)]
        return result

    def update_helium_skfs(self):
        #devs = self.get_device_request_data()
        # need another query to update max_copies & join_eui...?
        active_devices = self.fetch_active_devices()
        devices = list(map(self.get_device_activation, active_devices))
        for device in devices:
            dev_eui = device['devEui']
            # join_eui = device['join_eui']
            dev_addr = device['devAddr']
            nws_key = device['nwkSEncKey']
            query = """
                INSERT INTO helium_skfs (dev_eui, dev_addr, nws_key)
                VALUES ('{0}', '{1}', '{2}')
                ON CONFLICT (dev_eui) DO
                UPDATE SET
                    dev_eui='{0}',
                    dev_addr='{1}',
                    nws_key='{2}'
                WHERE helium_skfs.dev_eui='{0}';
            """.format(dev_eui, dev_addr, nws_key)
            self.db_transaction(query)
            print(query)

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
                        req = ujson.loads(MessageToJson(pl))
                        if 'method' not in req.keys():
                            continue

                        match req['method']:
                            case 'Create':
                                print('==========[ API=DEVICE Create DEVICE EUIs... ]==========')
                                print(MessageToJson(pl))
                                self.add_device_euis(req['metadata'])
                            case 'Delete':
                                print('==========[ API=DEVICE Delete DEVICE EUIs... ]==========')
                                print(MessageToJson(pl))
                                self.remove_device_euis(req['metadata'])
                            case 'Update':
                                print('==========[ API=DEVICE Update DEVICE EUIs... ]==========')
                                print(MessageToJson(pl))
                                self.update_device_euis(req['metadata'])

            except Exception as err:
                print(f'api_stream_requests: {err}')
                pass

    def add_device_euis(self, data: dict):
        """
        TODO:
            on device being added using chirpstack webui or api
            - add device euis to hpr
            - add device to helium_devices db
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        dev_eui, join_eui = self.get_device_request(device)
        print(f'Add Device EUIs: {dev_eui}, {join_eui}')

        query = """
            INSERT INTO helium_skfs (dev_eui, join_eui)
            VALUES ('{}', '{}')
            ON CONFLICT (dev_eui) DO NOTHING;
        """.format(dev_eui, join_eui)
        self.db_transaction(query)

        cmd = f'hpr route euis add -d {dev_eui} -a {join_eui} --route-id {self.route_id} --commit'
        Helium.config_service_cli(cmd)
        print('==[ ^ ADD EUIS debug... ^ ]==>')
        return

    def remove_device_euis(self, data: dict):
        """
        TODO:
            on device being removed using chirpstack webui or api
            - call device from helium_devices db on delete and remove from hpr device euis
            - call devaddr and nws keys to be removed from
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        print(f'Remove Device: {device}')

        query = "SELECT * FROM helium_skfs WHERE dev_eui='{}';".format(device)
        # print(query)
        data = client.db_fetch(query)[0]

        if data['dev_addr'] is not None and data['nws_key'] is not None:
            dev_addr = data['dev_addr']  # this should be a string..
            nws_key = data['nws_key']    # this should be a string..
            # if set remove dev_addr and nws_key from skfs's
            cmd = f'hpr route skfs remove -r {self.route_id} -d {dev_addr} -s {nws_key} -c'
            Helium.config_service_cli(cmd)
            print(f'Removing SKFS -> {cmd}')

        dev_eui = data['dev_eui']  # this should be a string..
        join_eui = data['join_eui']  # this should be a string..
        # remove device eui and join eui for device from router.
        cmd = f'hpr route euis remove -d {dev_eui} -a {join_eui} --route-id {self.route_id} -c'
        Helium.config_service_cli(cmd)
        print(f'Removing EUIS -> {cmd}')
        return

    def update_device_euis(self, data: dict):
        """
        TODO:
            on device being disabled in chirpstack webui or api
            - remove from hpr device euis on disable toggle
            - add device euis to hpr on enable toggle
        """
        if 'dev_eui' not in data.keys():
            return

        device = data['dev_eui']
        is_disabled = data['is_disabled']
        dev_eui, join_eui = self.get_device_request(device)
        if is_disabled == 'true':
            cmd = f'hpr route euis remove -d {dev_eui} -a {join_eui} --route-id {self.route_id} -c'
        else:
            cmd = f'hpr route euis add -d {dev_eui} -a {join_eui} --route-id {self.route_id} -c'
        Helium.config_service_cli(cmd)
        print('==[ ^ UPDATE EUIS debug... ^ ]==>')
        return

    def device_stream_event(self):
        stream_key = "device:stream:event"
        last_id = '0'
        while True:
            try:
                resp = rdb.xread({stream_key: last_id}, count=1, block=0)

                for message in resp[0][1]:
                    last_id = message[0]

                    if b"up" in message[1]:
                        b = message[1][b"up"]
                        pl = integration.UplinkEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE UP Event]==========')
                        print(MessageToJson(pl))

                    if b"join" in message[1]:
                        b = message[1][b"join"]
                        pl = integration.JoinEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE JOIN Event]==========')
                        print(MessageToJson(pl))

                    if b"ack" in message[1]:
                        b = message[1][b"ack"]
                        pl = integration.AckEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE ACK Event]==========')
                        print(MessageToJson(pl))

                    if b"txack" in message[1]:
                        b = message[1][b"txack"]
                        pl = integration.TxAckEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE TXACK Event]==========')
                        print(MessageToJson(pl))

                    if b"log" in message[1]:
                        b = message[1][b"log"]
                        pl = integration.LogEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE LOG Event]==========')
                        print(MessageToJson(pl))

                    if b"status" in message[1]:
                        b = message[1][b"status"]
                        pl = integration.StatusEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE STATUS Event]==========')
                        print(MessageToJson(pl))

                    if b"location" in message[1]:
                        b = message[1][b"location"]
                        pl = integration.LocationEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE LOCATION Event]==========')
                        print(MessageToJson(pl))

                    if b"integration" in message[1]:
                        b = message[1][b"integration"]
                        pl = integration.IntegrationEvent()
                        pl.ParseFromString(b)
                        print('==========[DEVICE INTEGRATION Event]==========')
                        print(MessageToJson(pl))

            except Exception as err:
                print(f'event_log_stream: {err}')
                pass

    def stream_meta(self):
        stream_key = 'stream:meta'
        last_id = '0'
        try:
            while True:
                resp = rdb.xread({stream_key: last_id}, count=1, block=0)

                for message in resp[0][1]:
                    last_id = message[0]

                    if b"up" in message[1]:
                        b = message[1][b"up"]
                        pl = meta.meta_pb2.UplinkMeta()
                        pl.ParseFromString(b)
                        print('==========[META = UPLINK]==========')
                        print(MessageToJson(pl))

                    if b"down" in message[1]:
                        b = message[1][b"down"]
                        pl = meta.meta_pb2.DownlinkMeta()
                        pl.ParseFromString(b)
                        print('==========[META = DOWNLINK]==========')
                        print(MessageToJson(pl))
        except Exception as err:
            print(f'stream_meta: {err}')
            pass

    def gw_stream_frame(self):
        stream_key = 'gw:stream:frame'
        last_id = '0'
        try:
            while True:
                resp = rdb.xread({stream_key: last_id}, count=1, block=0)

                for message in resp[0][1]:
                    last_id = message[0]

                    if b"up" in message[1]:
                        b = message[1][b"up"]
                        pl = api.frame_log_pb2.UplinkFrameLog()
                        pl.ParseFromString(b)
                        print("==========[UPLINK Gateway FRAME message...]==========")
                        print(MessageToJson(pl))
                        # client.publish("gateway/frame/up", MessageToJson(pl))

                    if b"down" in message[1]:
                        b = message[1][b"down"]
                        pl = api.frame_log_pb2.DownlinkFrameLog()
                        pl.ParseFromString(b)
                        print("==========[DOWNLINK Gateway FRAME message...]==========")
                        print(MessageToJson(pl))
                        # client.publish("gateway/frame/down", MessageToJson(pl))
        except Exception as err:
            print(f'gw_stream_frame: {err}')
            pass


class Helium:
    def config_service_cli(cmd: str):
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        if err:
            return err
        print(out)
        return out


if __name__ == '__main__':
    client = ChirpstackStreams(
        route_id=os.getenv('ROUTE_ID'),
        postgres_host=os.getenv('POSTGRES_HOST'),
        postgres_user=os.getenv('POSTGRES_USER'),
        postgres_pass=os.getenv('POSTGRES_PASS'),
        postgres_name=os.getenv('POSTGRES_DB'),
        chirpstack_host=os.getenv('CHIRPSTACK_SERVER'),
        chirpstack_token=os.getenv('CS_APIKEY')
    )

    def run_every(fn, interval):
        name = fn
        while True:
            fn()
            print(f'{name}: {time.ctime()}')
            time.sleep(interval)

    client.create_tables()

    with ThreadPoolExecutor(max_workers=3) as executor:
    #    executor.submit(run_every, client.update_tables, 600)  # seems to work..
    #    executor.submit(run_every, client.update_helium_skfs, 600)
        executor.submit(client.api_stream_requests)

#
#    device = client.get_device_request_data('e8e1e10001085071')
#    print(device, type(device))
#    if 'max_copies' in device.get('variables'):
#        print(device['devEui'], device['joinEui'], device['variables']['max_copies'])
##    client.update_helium_skfs()




#        executor.submit(client.device_stream_event)
#        executor.submit(client.stream_meta)


#    print(os.getenv('CS_TOKEN'))
#    device = 'e8e1e10001085071'
#    print(client.get_device_request(device))
#    device = '60c5a8fffe6d01fe'
#
#    query = """
#        SELECT dev_addr, dev_eui, join_eui, nws_key
#        FROM helium_devices
#        WHERE dev_eui = decode('{}', 'hex')
#    """.format(device)
#    device = 'd2bfb008ae7c74da'
#    query = """
#        SELECT dev_addr, dev_eui, join_eui, nws_key
#        FROM helium_hpr_skfs
#        WHERE dev_eui = '{}';
#    """.format(device)
#    data = client.db_fetch(query)[0]
#    print(data)

#    print(data, type(data))
#
#    if data['dev_addr'] is not None and data['nws_key'] is not None:
#        dev_addr = data['dev_addr'].hex()
#        nws_key = data['nws_key'].hex()
#        # remove dev_addr and nws_key from skfs's
#        cmd = f'hpr route skfs remove -r route_id -d {dev_addr} -s {nws_key} -c'
#        print(f'Remove Skfs: {cmd}')
#
#    dev_eui = data['dev_eui'].hex()
#    join_eui = data['join_eui'].hex()
#    cmd = f'hpr route euis remove -r route_id -d {dev_eui} -s {join_eui} -c'
#    print(f'Remove Euis: {cmd}')
#
#    #client.db_test_query(query)


#        # executor.submit(client.gw_stream_frame,)

#    client.db_test_query()
#    client.create_tables()
#    client.update_tables()
