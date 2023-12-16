try:
    from aioredis import Redis
except Exception as ex:
    from redis.asyncio import Redis


from backbone_orm import PostgresManager
from backbone_orm.postgres_manager import ConnectionConfig, DriverEnum

postgres = PostgresManager(
    default=DriverEnum.TEST,
    config=ConnectionConfig()
)

in_memory_redis_connection = Redis(host="127.0.0.1", port=6379)
