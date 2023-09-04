import os
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import psycopg2.extras
import redis
from math import ceil
import base64
# import grpc
from google.protobuf.json_format import MessageToDict  # MessageToJson
import ujson
from chirpstack_api import api, gw, integration, meta


# -----------------------------------------------------------------------------
# CHIRPSTACK REDIS CONNECTION
# -----------------------------------------------------------------------------
redis_server = os.getenv('REDIS_HOST')
rpool = redis.ConnectionPool(host=redis_server, port=6379, db=0)
rdb = redis.Redis(connection_pool=rpool, decode_responses=True)


class ChirpstackTenant:
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
                        data = MessageToDict(pl)
                        self.meta_up(data)

                    if b"down" in message[1]:
                        b = message[1][b"down"]
                        pl = meta.meta_pb2.DownlinkMeta()
                        pl.ParseFromString(b)
                        data = MessageToDict(pl)
                        self.meta_down(data)

        except Exception as exc:
            print(f'Error: {exc}')
            # log exception error here when adding logger
            pass

    def meta_up(self, data: dict):
        print('========== [ META = UPLINK ] ==========')
        print(ujson.dumps(data, indent=4))
        dev_eui = data['devEui']
        dupes = len(data['rxInfo'])
        dc = ceil(data['phyPayloadByteCount'] / 24)
        total_dc = dupes * dc
        query = """
            UPDATE helium_devices SET dc_used = (dc_used + {}) WHERE dev_eui='{}';
        """.format(total_dc, dev_eui)
        self.db_transaction(query)
        return

    def meta_down(self, data: dict):
        print('========== [ META = DOWNLINK ] ==========')
        print(ujson.dumps(data, indent=4))
        dev_eui = data['devEui']
        total_dc = ceil(data['phyPayloadByteCount'] / 24)
        query = """
            UPDATE helium_devices SET dc_used = (dc_used + {}) WHERE dev_eui='{}';
        """.format(total_dc, dev_eui)
        self.db_transaction(query)
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
                        self.event_up(MessageToDict(pl))

                    if b"join" in message[1]:
                        b = message[1][b"join"]
                        pl = integration.JoinEvent()
                        pl.ParseFromString(b)
                        self.event_join(MessageToDict(pl))

                    if b"ack" in message[1]:
                        b = message[1][b"ack"]
                        pl = integration.AckEvent()
                        pl.ParseFromString(b)
                        print('========== [ device ACK Event] ==========')
                        self.event_ack(MessageToDict(pl))

                    if b"txack" in message[1]:
                        b = message[1][b"txack"]
                        pl = integration.TxAckEvent()
                        pl.ParseFromString(b)
                        print('========== [ device TXACK Event] ==========')
                        self.event_txack(MessageToDict(pl))

                    if b"log" in message[1]:
                        b = message[1][b"log"]
                        pl = integration.LogEvent()
                        pl.ParseFromString(b)
                        print('========== [ device LOG Event] ==========')
                        self.event_log(MessageToDict(pl))

                    if b"status" in message[1]:
                        b = message[1][b"status"]
                        pl = integration.StatusEvent()
                        pl.ParseFromString(b)
                        self.event_status(MessageToDict(pl))

                    if b"location" in message[1]:
                        b = message[1][b"location"]
                        pl = integration.LocationEvent()
                        pl.ParseFromString(b)
                        print('========== [ device LOCATION Event] ==========')
                        self.event_location(MessageToDict(pl))

                    if b"integration" in message[1]:
                        b = message[1][b"integration"]
                        pl = integration.IntegrationEvent()
                        pl.ParseFromString(b)
                        print('========== [ device INTEGRATION Event] ==========')
                        self.event_integration(MessageToDict(pl))

            except Exception as err:
                print(f'ERROR device_stream_event: {err}')
                pass

    def event_up(self, data: dict):
        print('========== [ device UP Event] ==========')
        tenant_id = data['deviceInfo']['tenantId']
        tenant_name = data['deviceInfo']['tenantName']
        device_name = data['deviceInfo']['deviceName']
        num_dupes = len(data['rxInfo'])
        msg_bytes = ceil(len(base64.b64decode(data['data'])) / 24)
        total_dc = num_dupes * msg_bytes
        print('Tenant:', tenant_id, '\n' +
              'Name:', tenant_name, '\n' +
              'Device:', device_name, '\n' +
              'Dupes:', num_dupes, '\n' +
              'DC:', msg_bytes, '\n' +
              'Total DC:', total_dc)
        query = """
            UPDATE helium_tenant SET dc_balance = (dc_balance - {}) WHERE tenant_id = '{}';
        """.format(total_dc, tenant_id)
        self.db_transaction(query)
        return

    def event_join(self, data: dict):
        print('========== [ device JOIN Event] ==========')
        tenant_id = data['deviceInfo']['tenantId']
        tenant_name = data['deviceInfo']['tenantName']
        device_name = data['deviceInfo']['deviceName']
        dev_eui = data['deviceInfo']['devEui']
        total_dc = 1
        print('Tenant:', tenant_id, '\n' +
              'Name:', tenant_name, '\n' +
              'Device:', device_name, '\n' +
              'Dev_Eui:', dev_eui, '\n' +
              'Total DC:', total_dc)
        query = """
            UPDATE helium_tenant SET dc_balance = (dc_balance - {}) WHERE tenant_id = '{}';
        """.format(total_dc, tenant_id)
        self.db_transaction(query)
        # print(ujson.dumps(data, indent=4))
        return

    def event_ack(self, data: dict):
        print(ujson.dumps(data, indent=4))
        return

    def event_txack(self, data: dict):
        print(ujson.dumps(data, indent=4))
        return

    def event_log(self, data: dict):
        print(ujson.dumps(data, indent=4))
        return

    def event_status(self, data: dict):
        print('========== [ device STATUS Event] ==========')
        tenant_id = data['deviceInfo']['tenantId']
        tenant_name = data['deviceInfo']['tenantName']
        device_name = data['deviceInfo']['deviceName']
        dev_eui = data['deviceInfo']['devEui']
        total_dc = 1
        print('Tenant:', tenant_id, '\n' +
              'Name:', tenant_name, '\n' +
              'Device:', device_name, '\n' +
              'Dev_Eui:', dev_eui, '\n' +
              'Total DC:', total_dc)
        query = """
            UPDATE helium_tenant SET dc_balance = (dc_balance - {}) WHERE tenant_id = '{}';
        """.format(total_dc, tenant_id)
        self.db_transaction(query)
        # print(ujson.dumps(data, indent=4))
        return

    def event_location(self, data: dict):
        print(ujson.dumps(data, indent=4))
        return

    def event_integration(self, data: dict):
        print(ujson.dumps(data, indent=4))
        return

    """
    from helium_tenant_balance
    tenant_id, tenant_name, dc_balance, is_disabled
    """
