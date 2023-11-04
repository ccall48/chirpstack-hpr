import os
import psycopg2
import psycopg2.extras
import redis
from math import ceil
from google.protobuf.json_format import MessageToDict
from chirpstack_api import meta

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
        self.postges = f'postgresql://{self.pg_user}:{self.pg_pass}@{self.pg_host}:{self.pg_port}/{self.pg_name}?sslmode={self.pg_ssl_mode}'
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

        except Exception as exc:
            print(f'Error: {exc}')
            # log exception error here when adding logger
            pass

    def meta_up(self, data: dict):
        dev_eui = data['devEui']
        dupes = len(data['rxInfo'])
        dc = ceil(data['phyPayloadByteCount'] / 24)
        total_dc = dupes * dc
        print(f'dev_eui: {dev_eui} | MSG DC {dc} | Dupes: {dupes} | Total DC: {total_dc}')
        query = """
            UPDATE helium_devices SET dc_used = (dc_used + {}) WHERE dev_eui='{}';
        """.format(total_dc, dev_eui)
        self.db_transaction(query)
        return
