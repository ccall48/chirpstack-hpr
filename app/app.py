import os
import asyncio
import logging
import time

from DatabasePool import Database
from ChirpHeliumRequestsRpc import ChirpstackStreams
from ChirpHeliumKeysRpc import ChirpDeviceKeys
from ChirpHeliumTenant import ChirpstackTenant
from ChirpHeliumJoinRpc import ChirpstackJoins


logging.basicConfig(level=logging.INFO)


def build_dsn() -> str:
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASS')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT', 5432)
    name = os.getenv('POSTGRES_DB')
    ssl_mode = os.getenv('POSTGRES_SSL_MODE', 'allow')
    dsn = f'postgresql://{user}:{password}@{host}:{port}/{name}'
    return f'{dsn}?sslmode={ssl_mode}'


async def run_periodically(coro_fn, interval: int, name: str):
    """Run an async callable forever, every `interval` seconds."""
    while True:
        start = time.time()
        try:
            print(f'{time.ctime()} Executing: {name}, interval: {interval}s.')
            await coro_fn()
        except Exception as err:
            print(f'{name} Error: {err}')
        elapsed = time.time() - start
        await asyncio.sleep(max(0, interval - elapsed))


async def main():
    route_id = os.getenv('ROUTE_ID')
    chirpstack_host = os.getenv('CHIRPSTACK_SERVER')
    chirpstack_token = os.getenv('CHIRPSTACK_APIKEY')

    db = Database(build_dsn())
    await db.connect()

    events = ChirpstackJoins(
        route_id, db.pool, chirpstack_host, chirpstack_token)
    client_streams = ChirpstackStreams(
        route_id, db.pool, chirpstack_host, chirpstack_token)
    client_keys = ChirpDeviceKeys(
        route_id, db.pool, chirpstack_host, chirpstack_token)
    tenant = ChirpstackTenant(
        route_id, db.pool, chirpstack_host, chirpstack_token)

    async def update_device_status():
        updates = []
        for dev_eui in await client_keys.fetch_all_devices():
            updates.append(await client_keys.get_merged_keys(dev_eui))
        print('\n'.join(updates))

    skfs_int = 60 * 5   # 5 minutes
    device_int = 60 * 5  # 5 minutes

    await client_streams.create_tables()

    try:
        await asyncio.gather(
            client_streams.api_stream_requests(),
            events.device_stream_event(),
            tenant.stream_meta(),
            run_periodically(update_device_status, device_int,
                             'update_device_status'),
            run_periodically(client_keys.helium_skfs_update, skfs_int,
                             'helium_skfs_update'),
        )
    finally:
        await db.close()


if __name__ == '__main__':
    asyncio.run(main())
