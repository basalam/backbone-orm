import aioredis
import asyncpg
import testing.postgresql

from backbone_orm.postgres_connection import PostgresConnection

in_memory_server = testing.postgresql.Postgresql()
__in_memory_postgres_connection = None


async def in_memory_postgres_connection():
    global __in_memory_postgres_connection
    if __in_memory_postgres_connection is None:
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=in_memory_server.settings["port"],
            user="postgres",
            database="test",
            timeout=0.1,
            server_settings=dict(jit="off"),
        )

        __in_memory_postgres_connection = PostgresConnection(connection=conn)

    return __in_memory_postgres_connection


in_memory_redis_connection = aioredis.Redis(host="127.0.0.1", port=6379)
