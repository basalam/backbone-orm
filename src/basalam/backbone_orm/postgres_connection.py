import traceback
from time import time
from typing import List, Tuple, Union, TYPE_CHECKING, Optional, Callable

import asyncpg as asyncpg
from asyncpg.transaction import Transaction
from pydantic import BaseModel

if TYPE_CHECKING:
    from basalam.backbone_orm.postgres_transaction import PostgresTransaction


class WildcardQueryNotAllowedException(Exception):
    pass


class QueryException(Exception):
    pass


class QueryProfile(BaseModel):
    execution_time: float
    query: str
    params: Union[List, Tuple] = []
    trace: List[str] = []


class PostgresConnection:

    def __init__(
            self,
            connection: asyncpg.Connection,
            debug_enabled: bool = False,
            allow_wildcard_queries: bool = False,
            transactions_enabled: bool = True,
    ) -> None:
        self.__connection: asyncpg.Connection = connection
        self.__transaction_level = 0
        self.__history: List[QueryProfile] = []
        self.__debug_enabled: bool = debug_enabled
        self.__allow_wildcard_queries: bool = allow_wildcard_queries
        self.__transactions_enabled: bool = transactions_enabled
        self.__active_transaction: Optional[Transaction] = None
        self.__active_transaction_callbacks: List[Callable] = []

    @property
    def history(self):
        return self.__history

    def enable_debug(self):
        self.__debug_enabled = True

    def disable_debug(self):
        self.__debug_enabled = False

    def transaction(self, isolation: Optional[str] = None) -> "PostgresTransaction":
        from basalam.backbone_orm.postgres_transaction import PostgresTransaction

        return PostgresTransaction(self, isolation)

    def add_transaction_callback(self, callback: Callable) -> None:
        self.__active_transaction_callbacks.append(callback)

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
                    for result in await self.__connection.fetch(query, *params)
                ]
            else:
                await self.__connection.execute(query, *params)
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
            traces = [f"{trace.filename}:{trace.lineno}" for trace in trace_back]
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
            self.__active_transaction = self.__connection.transaction(
                isolation=isolation
            )
            await self.__active_transaction.start()

        self.__transaction_level += 1

    async def rollback_transaction(self):
        self.__transaction_level -= 1

        if self.__is_end_of_transaction():
            await self.__active_transaction.rollback()
            self.__active_transaction = None
            self.__active_transaction_callbacks = []

    async def commit_transaction(self):
        self.__transaction_level -= 1

        if self.__is_end_of_transaction():
            await self.__active_transaction.commit()
            self.__active_transaction = None
            for callback in self.__active_transaction_callbacks: await callback()
            self.__active_transaction_callbacks = []

    def __is_wildcard_query(self, query: str) -> bool:
        return (
                (query.startswith("DELETE FROM") and "WHERE" not in query)
                or (query.startswith("delete from") and "where" not in query)
                or (query.startswith("UPDATE") and "WHERE" not in query)
                or (query.startswith("update") and "where" not in query)
        )

    async def close(self):
        await self.__connection.close()

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
