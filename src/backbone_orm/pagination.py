from abc import ABC, abstractmethod
from typing import List, Type, Callable, Dict, Optional

from .repository_abstract import RepositoryAbstract
from .parameters import Parameters
from pydantic import ConfigDict, BaseModel, Field
from pypika import functions
from pypika.queries import QueryBuilder


class PaginationResponse(BaseModel, ABC):
    data: List
    total: int
    per_page: int
    current_page: int
    last_page: int
    from_: int = Field(alias='from')
    to: int

    model_config = ConfigDict(populate_by_name=True)

    @classmethod
    @abstractmethod
    def repo(cls) -> Type[RepositoryAbstract]: pass

    @classmethod
    @abstractmethod
    def mapper(cls) -> Callable: pass

    @classmethod
    def relations(cls) -> List[str]: pass

    @classmethod
    async def make(
            cls,
            query: QueryBuilder,
            page: int,
            per_page: int,
            aggregate_query: Optional[QueryBuilder] = None,
            params: Optional[Parameters] = None,
            append: Dict = None,
    ) -> "PaginationResponse":
        main_query = query.__copy__().limit(per_page).offset((page - 1) * per_page)
        entities = await (cls.repo()).get(query=main_query, params=params, relations=cls.relations())

        if aggregate_query is None:
            aggregate_query = query.__copy__()
            aggregate_query._selects = []
            aggregate_query._orderbys = []
            aggregate_query = aggregate_query.select(functions.Count('*').as_('total'))

        aggregations = (await (cls.repo()).execute_and_fetch(query=aggregate_query, params=params))[0]

        return cls(
            data=[await (cls.mapper())(entity) for entity in entities],
            **aggregations,
            per_page=per_page,
            current_page=page,
            last_page=int(aggregations['total'] / per_page) + 1,
            from_=((page - 1) * per_page) + 1,
            to=((page - 1) * per_page) + len(entities),
            **(append or {})
        )

    @classmethod
    async def resource(
            cls,
            query: QueryBuilder,
            page: int,
            per_page: int,
            aggregate_query: Optional[QueryBuilder] = None,
            params: Optional[Parameters] = None,
            append: Dict = None,
    ) -> Dict:
        return (await cls.make(
            query=query,
            aggregate_query=aggregate_query,
            page=page,
            per_page=per_page,
            params=params,
            append=append,
        )).dict(by_alias=True)
