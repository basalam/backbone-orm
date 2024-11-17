from typing import Optional

from .postgres_connection import PostgresConnection


class PostgresTransaction:
    def __init__(
        self, connection: PostgresConnection, isolation: Optional[str] = None
    ):
        self.__isolation = isolation
        self.__connection = connection

    async def __aenter__(self):
        await self.__connection.begin_transaction(self.__isolation)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            await self.__connection.commit_transaction()
            return True
        else:
            await self.__connection.rollback_transaction()
            return False
