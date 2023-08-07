import os
import time
from concurrent.futures import ThreadPoolExecutor
from ChirpHeliumRequests import ChirpstackStreams
from ChirpHeliumKeys import ChirpDeviceKeys
# from ChirpHeliumTenant import ChirpstackTenant


if __name__ == '__main__':
    route_id = os.getenv('ROUTE_ID')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_pass = os.getenv('POSTGRES_PASS')
    postgres_name = os.getenv('POSTGRES_DB')
    chirpstack_host = os.getenv('CHIRPSTACK_SERVER')
    chirpstack_token = os.getenv('CS_APIKEY')

    client_streams = ChirpstackStreams(
        route_id=route_id,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_pass=postgres_pass,
        postgres_name=postgres_name,
        chirpstack_host=chirpstack_host,
        chirpstack_token=chirpstack_token
    )

    client_keys = ChirpDeviceKeys(
        route_id=route_id,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_pass=postgres_pass,
        postgres_name=postgres_name,
        chirpstack_host=chirpstack_host,
        chirpstack_token=chirpstack_token
    )

    def run_every(fn: str, interval: int):
        name = fn
        while True:
            start = time.time()
            fn()
            print(f'{time.ctime()} Executing {name}, sleeping {interval} seconds.')
            stop = time.time()
            time.sleep(interval - (stop - start))

    def update_device_keys():
        updates = list(map(client_keys.get_merged_keys, client_keys.fetch_all_devices()))
        print('\n'.join(updates))
        return

    interval = 60 * 30  # 30 minutes
    client_streams.create_tables()

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.submit(client_streams.api_stream_requests)
        #executor.submit(client_streams.device_stream_event)
        #executor.submit(client_streams.stream_meta)
        executor.submit(run_every, update_device_keys, interval)
