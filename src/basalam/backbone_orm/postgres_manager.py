import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict, Any, Tuple

import asyncpg
import testing.postgresql
from asyncpg import Connection
from asyncpg.pool import PoolAcquireContext
from pydantic.main import BaseModel

from .postgres_connection import PostgresConnection


class DriverEnum(Enum):
    TEST = 'test'
    SINGLE = 'single'
    POOL = 'pool'


class ConnectionConfig(BaseModel):
    pool_min_size: int = 5
    pool_max_size: int = 25
    pool_acquire_timeout: int = 1
    pool_max_inactive_connection_lifetime: int = 10
    user: str = ''
    password: str = ''
    host: str = ''
    port: int = 0
    db: str = ''
    timeout: float = 1
    server_settings: Dict = dict(jit="off")
    test_host: str = "127.0.0.1"
    test_user: str = "postgres"
    test_db: str = "test"


class DriverAbstract(ABC):

    def __init__(self, config: ConnectionConfig) -> None:
        pass

    @abstractmethod
    def acquire(self, *args, **kwargs) -> PostgresConnection:
        pass

    @abstractmethod
    def release(self, *args, **kwargs) -> None:
        pass


class TestDriver(DriverAbstract):

    def __init__(self, config: ConnectionConfig) -> None:
        super().__init__(config)
        self.__config = config
        self.__server: Optional[testing.postgresql.Postgresql] = None
        self.__connection: Optional[PostgresConnection] = None

    def server(self) -> testing.postgresql.Postgresql:
        self.__server = self.__server or testing.postgresql.Postgresql()
        return self.__server

    async def acquire(self) -> PostgresConnection:
        self.__connection = self.__connection or PostgresConnection(
            await asyncpg.connect(
                host=self.__config.test_host,
                port=self.server().settings["port"],
                user=self.__config.test_user,
                database=self.__config.test_db,
                timeout=self.__config.timeout,
                server_settings=self.__config.server_settings,
            )
        )
        return self.__connection

    async def release(self) -> None:
        if self.__connection is not None: await self.__connection.close()
        if self.__server is not None: self.__server.stop()


class PoolDriver(DriverAbstract):

    def __init__(self, config: ConnectionConfig) -> None:
        super().__init__(config)
        self.__config = config
        self.__is_creating_pool: bool = False
        self.__pool: Optional[asyncpg.Pool] = None
        self.__pool_lock = asyncio.Lock()
        self.__acquires: Dict[str, Tuple[PostgresConnection, PoolAcquireContext]] = {}

    async def acquire(self, key: Any) -> PostgresConnection:
        if key not in self.__acquires:
            connection = await (await self.pool()).acquire(timeout=self.__config.pool_acquire_timeout)
            self.__acquires[key] = (PostgresConnection(connection=connection), connection)
        return self.__acquires[key][0]

    async def release(self, key: Any) -> None:
        if key in self.__acquires.keys():
            await (await self.pool()).release(self.__acquires[key][1])
            del self.__acquires[key]

    async def pool(self):
        if self.__is_creating_pool:
            await asyncio.sleep(0.1)
            return await self.pool()

        if self.__pool is None:
            try:
                self.__is_creating_pool = True
                self.__pool = await asyncpg.create_pool(
                    min_size=self.__config.pool_min_size,
                    max_size=self.__config.pool_max_size,
                    max_inactive_connection_lifetime=self.__config.pool_max_inactive_connection_lifetime,
                    user=self.__config.user,
                    password=self.__config.password,
                    host=self.__config.host,
                    port=self.__config.port,
                    database=self.__config.db,
                    timeout=self.__config.timeout,
                    server_settings=dict(**self.__config.server_settings),
                )
                self.__is_creating_pool = False
            except Exception as e:
                self.__is_creating_pool = False
                raise e
        return self.__pool


class SingleDriver(DriverAbstract):

    def __init__(self, config: ConnectionConfig) -> None:
        super().__init__(config)
        self.__config = config
        self.__connection: Optional[Tuple[PostgresConnection, Connection]] = None

    async def acquire(self, *args, **kwargs) -> PostgresConnection:
        if self.__connection is None or self.__connection[1].is_closed():
            connection = await asyncpg.connect(
                user=self.__config.user,
                password=self.__config.password,
                host=self.__config.host,
                port=self.__config.port,
                database=self.__config.db,
                timeout=self.__config.timeout,
                server_settings=dict(**self.__config.server_settings),
            )
            self.__connection = (PostgresConnection(connection), connection)

        return self.__connection[0]

    def release(self, *args, **kwargs) -> None:
        self.__connection[1].close()
        del self.__connection


class PostgresManager:

    def __init__(self, config: ConnectionConfig, default: Optional[DriverEnum] = SingleDriver) -> None:
        self.__config = config
        self.__default = default
        self.__drivers = {
            DriverEnum.POOL  : PoolDriver(config),
            DriverEnum.SINGLE: SingleDriver(config),
            DriverEnum.TEST  : TestDriver(config),
        }

    async def acquire(self, driver: Optional[DriverEnum] = None, *args, **kwargs) -> PostgresConnection:
        return await self.__drivers[driver or self.__default].acquire(*args, **kwargs)

    async def release(self, driver: Optional[DriverEnum], *args, **kwargs) -> None:
        return await self.__drivers[driver or self.__default].release(*args, **kwargs)
