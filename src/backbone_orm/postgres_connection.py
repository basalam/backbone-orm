import os
import traceback
from time import time
from typing import List, Tuple, Union, TYPE_CHECKING, Optional, Any

import asyncpg as asyncpg
from asyncpg import Connection
from asyncpg.transaction import Transaction
from pydantic import BaseModel

if TYPE_CHECKING:
    from backbone_orm.postgres_transaction import PostgresTransaction


class WildcardQueryNotAllowedException(Exception):
    pass


class QueryException(Exception):
    pass


class QueryProfile(BaseModel):
    execution_time: int
    query: str
    bindings: Union[List, Tuple] = []
    trace = List[str]

    class Config:
        arbitrary_types_allowed = True


class PostgresConnection:
    def __init__(
        self,
        user: str,
        password: Optional[str],
        host: str,
        port: int,
        database: str,
        timeout_in_seconds: float,
        default_schema: Optional[str] = None,
        debug_enabled: bool = False,
        allow_wildcard_queries: bool = False,
        transactions_enabled: bool = True,
    ) -> None:
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__database = database
        self.__timeout_in_seconds = timeout_in_seconds
        self.__default_schema = default_schema

        self.__connection: Optional = None

        self.__transaction_level = 0

        self.__history: List[QueryProfile] = []
        self.__debug_enabled: bool = debug_enabled
        self.__allow_wildcard_queries: bool = allow_wildcard_queries
        self.__transactions_enabled: bool = transactions_enabled
        self.__active_transaction: Optional[Transaction] = None

    async def connection(self) -> Connection:
        if self.__connection is None or self.__connection.is_closed():
            self.__connection: Connection = await asyncpg.connect(
                user=self.__user,
                password=self.__password,
                host=self.__host,
                port=self.__port,
                database=self.__database,
                timeout=self.__timeout_in_seconds,
                server_settings=dict(
                    **(
                        dict(search_path=self.__default_schema)
                        if self.__default_schema is not None
                        else {}
                    ),
                    jit="off",
                ),
            )
        return self.__connection

    @property
    def history(self):
        return self.__history

    def enable_debug(self):
        self.__debug_enabled = True

    def disable_debug(self):
        self.__debug_enabled = False

    def transaction(
        self, set_transaction_isolation: bool = False
    ) -> "PostgresTransaction":
        from backbone_orm.postgres_transaction import PostgresTransaction

        return PostgresTransaction(self, set_transaction_isolation)

    async def execute(self, query: str, params=None, fetch: bool = False):
        if params is None:
            params = []

        if self.__is_wildcard_query(query) and not self.__allow_wildcard_queries:
            raise WildcardQueryNotAllowedException(query)

        start = time()
        try:
            if fetch:
                results = [
                    dict(result)
                    for result in await (await self.connection()).fetch(query, *params)
                ]
            else:
                await (await self.connection()).execute(query, *params)
                results = None
        except (
            asyncpg.exceptions.PostgresSyntaxError,
            asyncpg.exceptions.UndefinedParameterError,
            asyncpg.exceptions.InterfaceError,
            asyncpg.exceptions.NotNullViolationError,
            asyncpg.exceptions.DataError,
        ) as exception:
            raise QueryException(f"{exception} --- Executed Query: {query}", params)

        execution_time = time() - start

        if self.__debug_enabled:
            trace_back: List[traceback.FrameSummary] = traceback.extract_stack()
            base_path = os.path.dirname(os.path.abspath(__file__ + "/../../..")) + "/."
            traces = [
                f"{trace.filename.replace(base_path, '')}:{trace.lineno}"
                for trace in trace_back
            ]
            traces = [
                trace
                for trace in traces
                if not any(
                    [
                        "postgres.py" in trace,
                        "infrastructure/database" in trace,
                    ]
                )
            ]
            self.__history.append(
                QueryProfile(
                    execution_time=execution_time,
                    query=query,
                    params=params,
                    trace=traces,
                )
            )

        return results

    async def execute_and_fetch(self, query: str, params=None):
        return await self.execute(query, params, fetch=True)

    async def begin_transaction(self, isolation: Optional[str] = None):

        if not self.__transactions_enabled:
            return

        if self.__is_start_of_transaction():
            self.__active_transaction = (await self.connection()).transaction(
                isolation=isolation
            )
            await self.__active_transaction.start()

        self.__transaction_level += 1

    async def rollback_transaction(self):

        self.__transaction_level -= 1

        if self.__is_end_of_transaction():
            await self.__active_transaction.rollback()
            self.__active_transaction = None

    async def commit_transaction(self):
        self.__transaction_level -= 1

        if self.__is_end_of_transaction():
            await self.__active_transaction.commit()
            self.__active_transaction = None

    def __is_wildcard_query(self, query: str) -> bool:
        return (
            (query.startswith("DELETE FROM") and "WHERE" not in query)
            or (query.startswith("delete from") and "where" not in query)
            or (query.startswith("UPDATE") and "WHERE" not in query)
            or (query.startswith("update") and "where" not in query)
        )

    async def close(self):
        await (await self.connection()).close()

    def enable_auto_commit(self):
        self.connection.autocommit = True

    def disable_auto_commit(self):
        self.connection.autocommit = False

    def __is_start_of_transaction(self):
        return self.__transaction_level == 0

    def __is_end_of_transaction(self):
        return self.__transaction_level == 0

    def allow_wildcard_queries(self):
        self.__allow_wildcard_queries = True

    def deny_wildcard_queries(self):
        self.__allow_wildcard_queries = False

    @property
    def transactions_enabled(self):
        return self.__transactions_enabled

    @property
    def is_in_transaction(self) -> bool:
        return self.__active_transaction is not None
