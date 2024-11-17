from abc import ABC, abstractmethod


class MigrationAbstract(ABC):
    @abstractmethod
    async def setup(self):
        pass

    async def teardown(self):
        pass
