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

#    def db_fetch(self, query):
#        with psycopg2.connect(self.postges) as con:
#            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#                cur.execute(query)
#                return cur.fetchall()

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


if __name__ == '__main__':
    client = CSTenant(
        route_id=os.getenv('ROUTE_ID'),
        postgres_host=os.getenv('POSTGRES_HOST'),
        postgres_user=os.getenv('POSTGRES_USER'),
        postgres_pass=os.getenv('POSTGRES_PASS'),
        postgres_name=os.getenv('POSTGRES_DB'),
        chirpstack_host=os.getenv('CHIRPSTACK_SERVER'),
        chirpstack_token=os.getenv('CS_APIKEY')
    )

    def main():
        with ThreadPoolExecutor(max_workers=2) as executor:
            #executor.submit(client.device_stream_event())
            executor.submit(client.stream_meta())

    main()


"""
========== [ UPLINK ] ==========
{
    "devEui": "24e124136c506271",
    "txInfo": {
        "frequency": 924400000,
        "modulation": {
            "lora": {
                "bandwidth": 125000,
                "spreadingFactor": 7,
                "codeRate": "CR_4_5"
            }
        }
    },
    "rxInfo": [
        {
            "gatewayId": "85cdd87a616b30f8",
            "uplinkId": 5489,
            "time": "2023-08-04T08:40:11Z",
            "rssi": -75,
            "snr": 13.8,
            "context": "ivdpgw==",
            "metadata": {
                "gateway_id": "11c6FeeFPUGJqdX7TN8LvUfF9xo7QAPv1TWFEyQ1jdjFsxHRGFT",
                "region_common_name": "AS923",
                "region_config_id": "as923_1c",
                "gateway_name": "raspy-malachite-butterfly"
            },
            "crcStatus": "CRC_OK"
        }
    ],
    "phyPayloadByteCount": 27,
    "macCommandByteCount": 1,
    "applicationPayloadByteCount": 13,
    "messageType": "UNCONFIRMED_DATA_UP"
}
========== [ DOWNLINK ] ==========
{
    "devEui": "24e124136c506271",
    "txInfo": {
        "frequency": 924400000,
        "power": 27,
        "modulation": {
            "lora": {
                "bandwidth": 125000,
                "spreadingFactor": 7,
                "polarizationInversion": true,
                "codeRate": "CR_4_5"
            }
        },
        "timing": {
            "delay": {
                "delay": "1s"
            }
        },
        "context": "ivdpgw=="
    },
    "phyPayloadByteCount": 15,
    "macCommandByteCount": 3,
    "messageType": "UNCONFIRMED_DATA_DOWN",
    "gatewayId": "85cdd87a616b30f8"
}
"""

"""
========== [ device UP Event] ==========
{
  "deduplicationId": "d078d70c-411a-475d-adca-c8436a0cfad9",
  "time": "2023-07-28T04:10:22Z",
  "deviceInfo": {
    "tenantId": "52f14cd4-c6f1-4fbd-8f87-4025e1d49242",
    "tenantName": "Admin WDRIoT",
    "applicationId": "95856ede-1238-4272-b616-41f18dc10fc3",
    "applicationName": "Milesight EM300-DI",
    "deviceProfileId": "8508772d-b04a-4d5f-ab3d-ab8d954f65c9",
    "deviceProfileName": "Milesight EM300-DI",
    "deviceName": "Slack Milesight EM300-DI",
    "devEui": "24e124136c506271",
    "tags": {
      "some": "tag"
    }
  },
  "devAddr": "78000025",
  "adr": true,
  "dr": 5,
  "fCnt": 967,
  "fPort": 85,
  "data": "AXVUA2e6AARoYgUAAQ==",
  "object": {
    "humidity": 49.0,
    "temperature": 18.6,
    "gpio": 1.0,
    "battery": 84.0
  },
  "rxInfo": [
    {
      "gatewayId": "85cdd87a616b30f8",
      "uplinkId": 16591,
      "time": "2023-07-28T04:10:22Z",
      "rssi": -86,
      "snr": 13.2,
      "context": "9SmVoA==",
      "metadata": {
        "gateway_id": "11c6FeeFPUGJqdX7TN8LvUfF9xo7QAPv1TWFEyQ1jdjFsxHRGFT",
        "region_common_name": "AS923",
        "region_config_id": "as923_1c",
        "gateway_name": "raspy-malachite-butterfly"
      },
      "crcStatus": "CRC_OK"
    }
  ],
  "txInfo": {
    "frequency": 923800000,
    "modulation": {
      "lora": {
        "bandwidth": 125000,
        "spreadingFactor": 7,
        "codeRate": "CR_4_5"
      }
    }
  }
}

========== [ device STATUS Event] ==========
{
  "deduplicationId": "2909d456-d255-48f5-9080-fa656b8366c2",
  "time": "2023-07-28T04:24:56Z",
  "deviceInfo": {
    "tenantId": "52f14cd4-c6f1-4fbd-8f87-4025e1d49242",
    "tenantName": "Admin WDRIoT",
    "applicationId": "de105f9b-a152-4071-ba79-cf681ee9cb5a",
    "applicationName": "Browan Industrial Trackers",
    "deviceProfileId": "66e67a63-53fc-4f45-86c6-4614851adbc2",
    "deviceProfileName": "Browan Industrial Tracker",
    "deviceName": "EXA66C",
    "devEui": "e8e1e10001085028"
  },
  "margin": -3,
  "batteryLevel": 73.62205
}

========== [ device LOG Event] ==========
{
  "time": "2023-07-28T04:25:24Z",
  "deviceInfo": {
    "tenantId": "52f14cd4-c6f1-4fbd-8f87-4025e1d49242",
    "tenantName": "Admin WDRIoT",
    "applicationId": "95856ede-1238-4272-b616-41f18dc10fc3",
    "applicationName": "Milesight EM300-DI",
    "deviceProfileId": "8508772d-b04a-4d5f-ab3d-ab8d954f65c9",
    "deviceProfileName": "Milesight EM300-DI",
    "deviceName": "Slack Milesight EM300-DI",
    "devEui": "24e124136c506271",
    "tags": {
      "some": "tag"
    }
  },
  "level": "WARNING",
  "code": "UPLINK_F_CNT_RETRANSMISSION",
  "description": "Uplink was flagged as re-transmission / frame-counter did not increment",
  "context": {
    "deduplication_id": "6d673369-ff94-486e-876a-7fb62eac7c1c"
  }
}

========== [ device JOIN Event] ========== | multiple gw/hs relay
{
  "deduplicationId": "c01f6ac4-35d1-40e8-93f7-cdcf2d9bb1e8",
  "time": "2023-07-28T05:12:19Z",
  "deviceInfo": {
    "tenantId": "52f14cd4-c6f1-4fbd-8f87-4025e1d49242",
    "tenantName": "Admin WDRIoT",
    "applicationId": "b593b5a2-e906-4804-a227-9b338a7c162d",
    "applicationName": "Vega Si11 Pulse Counter",
    "deviceProfileId": "7e518a10-feb3-4c26-95f7-6174fddc8e0f",
    "deviceProfileName": "Vega Si11 Pulse Counter",
    "deviceName": "Vega Si11 Cory",
    "devEui": "363335325d385402"
  },
  "devAddr": "78000024"
}

========== [ device UP Event] ========== MULTI GW example...
{
  "deduplicationId": "24d770db-2ee1-49a0-9be4-87cdf68fef9d",
  "time": "2023-07-28T05:12:36Z",
  "deviceInfo": {
    "tenantId": "52f14cd4-c6f1-4fbd-8f87-4025e1d49242",
    "tenantName": "Admin WDRIoT",
    "applicationId": "b593b5a2-e906-4804-a227-9b338a7c162d",
    "applicationName": "Vega Si11 Pulse Counter",
    "deviceProfileId": "7e518a10-feb3-4c26-95f7-6174fddc8e0f",
    "deviceProfileName": "Vega Si11 Pulse Counter",
    "deviceName": "Vega Si11 Cory",
    "devEui": "363335325d385402"
  },
  "devAddr": "78000024",
  "adr": true,
  "fPort": 3,
  "confirmed": true,
  "data": "AAQAAQEIAAEBMQABBRAAAQUMAAEBDQABAQ4AAQEPAAEBNwACWAI=",
  "object": {
    "pulse_1": 83948.805,
    "pulse_3": 83901.465,
    "readTime": 524545.0,
    "pktType": 0.0,
    "values": 0.0,
    "battery": 4.0,
    "ambient": 1.0,
    "pulse_2": 83906.585,
    "pulse_4": 83902.725
  },
  "rxInfo": [
    {
      "gatewayId": "745dee4b85efd6b5",
      "uplinkId": 60170,
      "time": "2023-07-28T05:12:36Z",
      "rssi": -134,
      "snr": -17.8,
      "context": "AlRaXA==",
      "metadata": {
        "gateway_id": "11uTnsXiM39tfvLp2R5LcRsxihE93xGTqB6X49pB579MiqCQGQY",
        "region_common_name": "AU915",
        "region_config_id": "au915_1",
        "gateway_name": "gigantic-clay-peacock"
      },
      "crcStatus": "CRC_OK"
    },
    {
      "gatewayId": "9d50790ad3474f20",
      "uplinkId": 382,
      "time": "2023-07-28T05:12:36Z",
      "rssi": -133,
      "snr": -17.0,
      "context": "8LO+Eg==",
      "metadata": {
        "gateway_id": "112KMEKXFqfav2ZHNAXBDVzchseqR1vWw6nEBA4E59i7SRUst9v8",
        "region_common_name": "AU915",
        "region_config_id": "au915_1",
        "gateway_name": "passive-ebony-lion"
      },
      "crcStatus": "CRC_OK"
    },
    {
      "gatewayId": "2864afc74baa57f1",
      "uplinkId": 41352,
      "time": "2023-07-28T05:12:36Z",
      "rssi": -56,
      "snr": 8.5,
      "context": "JA638g==",
      "metadata": {
        "gateway_id": "11kJam7LdWe43zeVm73ATLpSgdWL6ZQCqvXvYmUyt5UcZdWcxTV",
        "region_common_name": "AU915",
        "region_config_id": "au915_1",
        "gateway_name": "jovial-silver-hedgehog"
      },
      "crcStatus": "CRC_OK"
    },
    {
      "gatewayId": "278730733e003fa5",
      "uplinkId": 17156,
      "time": "2023-07-28T05:12:36Z",
      "rssi": -134,
      "snr": -15.5,
      "context": "5Q9MLg==",
      "metadata": {
        "gateway_id": "112CdoTKYDP8Z4eZtGstJqjbvpLWKsNRmQ6bMs5vh3jKuqCGTHHD",
        "region_common_name": "AU915",
        "region_config_id": "au915_1",
        "gateway_name": "main-tartan-opossum"
      },
      "crcStatus": "CRC_OK"
    },
    {
      "gatewayId": "b55836f42954d3e5",
      "uplinkId": 63394,
      "time": "2023-07-28T05:12:36Z",
      "rssi": -129,
      "snr": -16.8,
      "context": "Oh525g==",
      "metadata": {
        "gateway_id": "11cdrLUhWRn54tnbPs4mQEgnEmscvjfg3mwpp2kP8hS2UuPaCSk",
        "region_common_name": "AU915",
        "region_config_id": "au915_1",
        "gateway_name": "sweet-slate-scallop"
      },
      "crcStatus": "CRC_OK"
    },
    {
      "gatewayId": "12bb9a9d4186f13d",
      "uplinkId": 51275,
      "time": "2023-07-28T05:12:36Z",
      "rssi": -130,
      "snr": -13.2,
      "context": "5r350g==",
      "metadata": {
        "gateway_id": "112ZnbDiaDnJe9vArYCV3qjxy8weG1o8W6cCrQtBD4nKmzQofJjE",
        "region_common_name": "AU915",
        "region_config_id": "au915_1",
        "gateway_name": "rich-chambray-hare"
      },
      "crcStatus": "CRC_OK"
    },
    {
      "gatewayId": "1e7dca9c2bba40ff",
      "uplinkId": 45337,
      "time": "2023-07-28T05:12:36Z",
      "rssi": -117,
      "snr": -1.8,
      "context": "iKq8gg==",
      "metadata": {
        "gateway_id": "11wc7vqNQqgZETGtbti3626yJPVeam6MaJXTWxdW2g9MoAkjneE",
        "region_common_name": "AU915",
        "region_config_id": "au915_1",
        "gateway_name": "flat-taupe-rat"
      },
      "crcStatus": "CRC_OK"
    }
  ],
  "txInfo": {
    "frequency": 917600000,
    "modulation": {
      "lora": {
        "bandwidth": 125000,
        "spreadingFactor": 12,
        "codeRate": "CR_4_5"
      }
    }
  }
}

"""
