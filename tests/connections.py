import aioredis
import testing.postgresql

from backbone_orm.postgres_connection import PostgresConnection

in_memory_server = testing.postgresql.Postgresql()

in_memory_postgres_connection = PostgresConnection(
    host="127.0.0.1",
    port=in_memory_server.settings["port"],
    user="postgres",
    database="test",
    default_schema=None,
    password=None,
    timeout_in_seconds=0.1,
)

in_memory_redis_connection = aioredis.Redis(host="127.0.0.1", port=6379)
