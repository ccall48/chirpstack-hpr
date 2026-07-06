import asyncpg


class Database:
    """Holds a single asyncpg connection pool, shared across the app.

    The pool is created once at startup (``connect``) and closed once at
    shutdown (``close``). Callers acquire/release individual connections
    from ``pool`` per transaction.
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=1,   # do not hold idle connections open
                max_size=5,   # small ceiling; queries are short-lived
            )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
