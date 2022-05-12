from backbone_orm.postgres_connection import PostgresConnection


class PostgresTransaction:
    def __init__(
        self, connection: PostgresConnection, set_transaction_isolation: bool = False
    ):
        self.__transaction_isolation = set_transaction_isolation
        self.__connection = connection

    async def __aenter__(self):
        await self.__connection.begin_transaction(self.__transaction_isolation)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            await self.__connection.commit_transaction()
            return True
        else:
            await self.__connection.rollback_transaction()
            return False
