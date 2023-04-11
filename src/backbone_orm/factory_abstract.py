from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Type

from .model_abstract import T
from .repository_abstract import RepositoryAbstract

U = TypeVar("U", bound=RepositoryAbstract)


class FactoryAbstract(ABC, Generic[U]):
    @classmethod
    async def create(cls, args: dict = None, **kwargs) -> T:
        return await cls.repo().create_return(cls.make(args=args, **kwargs))

    @classmethod
    def make_many(cls, count: int, args: List[dict] = None, **kwargs):
        if args is None:
            args = []
        items = [cls.make(**kwargs) for _ in range(count)]
        if len(args) != 0:
            for index, item in enumerate(items):
                item_args: dict = args[index % len(args)]
                for key, value in item_args.items():
                    item.__setitem__(key, value)
        return items

    @classmethod
    async def create_many(
        cls, count: int, args: List[dict] = None, **kwargs
    ) -> List[T]:
        return await cls.repo().create_return_many(cls.make_many(count, args, **kwargs))

    @classmethod
    @abstractmethod
    def repo(cls) -> Type[U]:
        pass

    @classmethod
    @abstractmethod
    def make(cls, args: dict = None, **kwargs) -> dict:
        pass
