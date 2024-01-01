import os
import asyncpg
import asyncio


class Database:
    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            user=os.getenv('POSTGRES_USER', 'chirpstack'),
            host=os.getenv('POSTGRES_HOST', 'chirpstack-postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            database=os.getenv('POSTGRES_DB', 'chirpstack'),
            password=os.getenv('POSTGRES_PASS', 'chirpstack'),
            loop=asyncio.get_running_loop(),
        )

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
        pass
