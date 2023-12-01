import os
import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
# from ChirpHeliumRequests import ChirpstackStreams
from ChirpHeliumRequestsRpc import ChirpstackStreams
from ChirpHeliumKeys import ChirpDeviceKeys
from ChirpHeliumTenant import ChirpstackTenant


logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    route_id = os.getenv('ROUTE_ID')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_pass = os.getenv('POSTGRES_PASS')
    postgres_name = os.getenv('POSTGRES_DB')
    postgres_port = os.getenv('POSTGRES_PORT', 5432)
    postgres_ssl_mode = os.getenv('POSTGRES_SSL_MODE', 'allow')
    chirpstack_host = os.getenv('CHIRPSTACK_SERVER')
    chirpstack_token = os.getenv('CHIRPSTACK_APIKEY')

    client_streams = ChirpstackStreams(
        route_id=route_id,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_pass=postgres_pass,
        postgres_name=postgres_name,
        postgres_port=postgres_port,
        postgres_ssl_mode=postgres_ssl_mode,
        chirpstack_host=chirpstack_host,
        chirpstack_token=chirpstack_token,
    )

    client_keys = ChirpDeviceKeys(
        route_id=route_id,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_pass=postgres_pass,
        postgres_name=postgres_name,
        postgres_port=postgres_port,
        postgres_ssl_mode=postgres_ssl_mode,
        chirpstack_host=chirpstack_host,
        chirpstack_token=chirpstack_token,
    )

    tenant = ChirpstackTenant(
        route_id=route_id,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_pass=postgres_pass,
        postgres_name=postgres_name,
        postgres_port=postgres_port,
        postgres_ssl_mode=postgres_ssl_mode,
        chirpstack_host=chirpstack_host,
        chirpstack_token=chirpstack_token,
    )

    def run_every(fn: str, interval: int):
        name = str(fn)
        while True:
            try:
                start = time.time()
                print(f'{time.ctime()} Executing: {name}, sleeping: {interval} seconds.')
                fn()
                stop = time.time()
                time.sleep(interval - (stop - start))
            except Exception as err:
                print(f'{name} Error: {err}')
                pass

    def async_wrapper(corro):
        return asyncio.run(corro())

    def update_device_status():
        updates = list(map(client_keys.get_merged_keys, client_keys.fetch_all_devices()))
        print('\n'.join(updates))
        return

    skfs_int = 60 * 5   # 5 minutes
    device_int = 60 * 5  # 5 minutes

    client_streams.create_tables()

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.submit(async_wrapper, client_streams.api_stream_requests)
        executor.submit(tenant.stream_meta)
        executor.submit(run_every, client_keys.helium_skfs_update, skfs_int)
        executor.submit(run_every, update_device_status, device_int)
