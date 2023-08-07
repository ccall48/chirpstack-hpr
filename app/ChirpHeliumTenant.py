import os
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import psycopg2.extras
import redis
from math import ceil
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

    """
    #    def db_fetch(self, query):
    #        with psycopg2.connect(self.postges) as con:
    #            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    #                cur.execute(query)
    #                return cur.fetchall()
    """

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
                        # print('========== [ UPLINK ] ==========')
                        data = MessageToDict(pl)
                        self.meta_up(data)

                    if b"down" in message[1]:
                        b = message[1][b"down"]
                        pl = meta.meta_pb2.DownlinkMeta()
                        pl.ParseFromString(b)
                        # print('========== [ DOWNLINK ] ==========')
                        data = MessageToDict(pl)
                        self.meta_down(data)

        except Exception as exc:
            print(f'Error: {exc}')
            # log exception error here when adding logger
            pass

    def meta_up(self, data: dict):
        print('========== [ UPLINK ] ==========')
        print(ujson.dumps(data, indent=4))
        return

    def meta_down(self, data: dict):
        print('========== [ DOWNLINK ] ==========')
        print(ujson.dumps(data, indent=4))
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
                        print('========== [ device UP Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_up(MessageToDict(pl))

                    if b"join" in message[1]:
                        b = message[1][b"join"]
                        pl = integration.JoinEvent()
                        pl.ParseFromString(b)
                        print('========== [ device JOIN Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_join(MessageToDict(pl))

                    if b"ack" in message[1]:
                        b = message[1][b"ack"]
                        pl = integration.AckEvent()
                        pl.ParseFromString(b)
                        print('========== [ device ACK Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_ack(MessageToDict(pl))

                    if b"txack" in message[1]:
                        b = message[1][b"txack"]
                        pl = integration.TxAckEvent()
                        pl.ParseFromString(b)
                        print('========== [ device TXACK Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_txack(MessageToDict(pl))

                    if b"log" in message[1]:
                        b = message[1][b"log"]
                        pl = integration.LogEvent()
                        pl.ParseFromString(b)
                        print('========== [ device LOG Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_log(MessageToDict(pl))

                    if b"status" in message[1]:
                        b = message[1][b"status"]
                        pl = integration.StatusEvent()
                        pl.ParseFromString(b)
                        print('========== [ device STATUS Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_status(MessageToDict(pl))

                    if b"location" in message[1]:
                        b = message[1][b"location"]
                        pl = integration.LocationEvent()
                        pl.ParseFromString(b)
                        print('========== [ device LOCATION Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_location(MessageToDict(pl))

                    if b"integration" in message[1]:
                        b = message[1][b"integration"]
                        pl = integration.IntegrationEvent()
                        pl.ParseFromString(b)
                        print('========== [ device INTEGRATION Event] ==========')
                        #msg = ujson.loads(MessageToJson(pl))
                        # print(ujson.dumps(msg, indent=2))
                        self.event_integration(MessageToDict(pl))

            except Exception as err:
                print(f'ERROR device_stream_event: {err}')
                pass

    def event_up(self, data: dict):
        #tenant = data['deviceInfo']['tenantId']
        #no_msgs = len(data['rxInfo'])
        #msg_bytes = data['phyPayloadByteCount']
        #total_dc = no_msgs * (msg_bytes / 24)
        #print(f'Tenant: {tenant} | heard by {no_msgs} hotspots | bytes {msg_bytes} | total DC {total_dc}')
        print(ujson.dumps(data, indent=4))
        #print(data)
        return

    def event_join(self, data: dict):
        print(ujson.dumps(data, indent=4))
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
        print(ujson.dumps(data, indent=4))
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
