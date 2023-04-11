import aioredis

from backbone_orm import PostgresManager
from backbone_orm.postgres_manager import ConnectionConfig, DriverEnum

postgres = PostgresManager(
    default=DriverEnum.TEST,
    config=ConnectionConfig()
)

in_memory_redis_connection = aioredis.Redis(host="127.0.0.1", port=6379)
