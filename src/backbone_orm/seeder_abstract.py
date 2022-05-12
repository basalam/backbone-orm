from abc import ABC, abstractmethod


class SeederAbstract(ABC):
    @abstractmethod
    async def handle(self):
        pass
